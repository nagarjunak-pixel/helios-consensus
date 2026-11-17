"""Segmented Binary Write-Ahead Log (`SegmentedWAL`) & Fault-Injecting Disk (`SimulatedDiskFS`).

Designed around the real storage failure modes studied in *All File Systems Are Not Created
Equal* (OSDI '14) and FoundationDB/Pebble testing:

1. **Page-cache vs durable-platter split (`SimulatedDiskFS`)**: ``append`` writes into the
   volatile OS page cache; only ``fsync`` promotes bytes to durable platters. A simulated
   ``power_cut`` drops un-fsynced writes (or leaves a deterministic torn prefix of the
   in-flight tail), catching any code path that replies to a Raft RPC without syncing.
2. **Binary C++20 framing**: Every log entry, ``HardState`` update, and log-suffix
   truncation is encoded via ``HeliosKernelBridge.wal_encode_record`` (`25`-byte binary
   header protected by Slicing-by-8 Castagnoli CRC32C) and recovered via ``wal_scan_frames``.
3. **Append-only segment rotation & compaction GC**: Active segments roll at
   ``max_segment_bytes`` (`00000001.wal`, `00000002.wal`, ...). Taking a snapshot writes an
   atomic CRC-protected snapshot image and unlinks closed segments strictly older than the
   snapshot index. Middle-segment corruption raises ``WalCorruptionError``; active-tail torn
   writes are cleanly truncated back to the last valid frame boundary.
"""

from __future__ import annotations

import json
import os
import random
import struct
from pathlib import Path
from typing import Protocol

from helios_consensus.core.messages import Entry, EntryKind, HardState, Ready, Snapshot
from helios_consensus.core.storage import snapshot_from_json, snapshot_to_json
from helios_consensus.kernels.native_ring import WalStopReason, get_helios_bridge

_KIND_TO_CODE = {
    EntryKind.NORMAL: 0,
    EntryKind.NOOP: 1,
    EntryKind.CONFIG: 2,
}
_CODE_TO_KIND = {v: k for k, v in _KIND_TO_CODE.items()}
_REC_HARD_STATE = 3
_REC_TRUNCATE_FROM = 4
_HS_STRUCT = struct.Struct("<QqQ")  # term (u64), vote (-1 for None, i64), commit (u64)


class WalCorruptionError(RuntimeError):
    """Raised when CRC32C detects bitrot inside a sealed or non-tail WAL frame."""


class DiskFS(Protocol):
    def list_files(self) -> list[str]: ...

    def exists(self, name: str) -> bool: ...

    def size(self, name: str) -> int: ...

    def read(self, name: str) -> bytes: ...

    def append(self, name: str, data: bytes, *, sync: bool) -> None: ...

    def truncate(self, name: str, length: int, *, sync: bool) -> None: ...

    def atomic_replace(self, name: str, data: bytes, *, sync: bool) -> None: ...

    def remove(self, name: str) -> None: ...


class PosixDiskFS:
    """Real OS directory backend using ``os.fsync`` and ``os.replace``."""

    def __init__(self, directory: Path) -> None:
        self.dir = directory
        self.dir.mkdir(parents=True, exist_ok=True)

    def _sync_dir(self) -> None:
        fd = os.open(self.dir, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def list_files(self) -> list[str]:
        return sorted(p.name for p in self.dir.iterdir() if p.is_file())

    def exists(self, name: str) -> bool:
        return (self.dir / name).exists()

    def size(self, name: str) -> int:
        p = self.dir / name
        return p.stat().st_size if p.exists() else 0

    def read(self, name: str) -> bytes:
        return (self.dir / name).read_bytes()

    def append(self, name: str, data: bytes, *, sync: bool) -> None:
        with (self.dir / name).open("ab") as fh:
            fh.write(data)
            if sync:
                fh.flush()
                os.fsync(fh.fileno())

    def truncate(self, name: str, length: int, *, sync: bool) -> None:
        with (self.dir / name).open("r+b") as fh:
            fh.truncate(length)
            if sync:
                fh.flush()
                os.fsync(fh.fileno())

    def atomic_replace(self, name: str, data: bytes, *, sync: bool) -> None:
        target = self.dir / name
        tmp = self.dir / f".{name}.tmp"
        with tmp.open("wb") as fh:
            fh.write(data)
            if sync:
                fh.flush()
                os.fsync(fh.fileno())
        os.replace(tmp, target)
        if sync:
            self._sync_dir()

    def remove(self, name: str) -> None:
        p = self.dir / name
        if p.exists():
            p.unlink()


class SimulatedDiskFS:
    """In-memory fault-injecting filesystem modeling volatile page cache vs durable platters."""

    def __init__(self, *, seed: int = 1) -> None:
        self._rng = random.Random(seed)
        self._durable: dict[str, bytearray] = {}
        self._cache: dict[str, bytearray] = {}
        self.fsync_calls = 0
        self.power_cuts = 0

    def list_files(self) -> list[str]:
        return sorted(self._cache)

    def exists(self, name: str) -> bool:
        return name in self._cache

    def size(self, name: str) -> int:
        return len(self._cache[name]) if name in self._cache else 0

    def read(self, name: str) -> bytes:
        if name not in self._cache:
            raise FileNotFoundError(name)
        return bytes(self._cache[name])

    def append(self, name: str, data: bytes, *, sync: bool) -> None:
        buf = self._cache.setdefault(name, bytearray())
        buf.extend(data)
        self._durable.setdefault(name, bytearray())
        if sync:
            self._durable[name] = bytearray(buf)
            self.fsync_calls += 1

    def truncate(self, name: str, length: int, *, sync: bool) -> None:
        if name not in self._cache:
            raise FileNotFoundError(name)
        del self._cache[name][length:]
        if sync:
            self._durable[name] = bytearray(self._cache[name])
            self.fsync_calls += 1

    def atomic_replace(self, name: str, data: bytes, *, sync: bool) -> None:
        self._cache[name] = bytearray(data)
        if sync:
            self._durable[name] = bytearray(data)
            self.fsync_calls += 1

    def remove(self, name: str) -> None:
        self._cache.pop(name, None)
        self._durable.pop(name, None)

    def fsync_all(self) -> None:
        self._durable = {k: bytearray(v) for k, v in self._cache.items()}
        self.fsync_calls += 1

    def power_cut(self, *, tear_unsynced_tail: bool = True) -> int:
        """Simulate sudden power loss: discard un-fsynced cache or leave a torn partial tail."""
        self.power_cuts += 1
        torn_bytes = 0
        recovered: dict[str, bytearray] = {}
        for name, d_buf in self._durable.items():
            c_buf = self._cache.get(name, d_buf)
            if tear_unsynced_tail and len(c_buf) > len(d_buf):
                unsynced = len(c_buf) - len(d_buf)
                # Keep a deterministic partial prefix < full unsynced region (torn sector).
                partial = self._rng.randint(0, max(0, min(unsynced - 1, 19)))
                merged = bytearray(d_buf)
                merged.extend(c_buf[len(d_buf) : len(d_buf) + partial])
                torn_bytes += partial
                recovered[name] = merged
            else:
                recovered[name] = bytearray(d_buf)
        self._cache = {k: bytearray(v) for k, v in recovered.items()}
        self._durable = {k: bytearray(v) for k, v in recovered.items()}
        return torn_bytes

    def inject_bitrot(self, name: str, byte_offset: int, mask: int = 0x01) -> None:
        """Flip bit(s) directly on both durable platter and cache to simulate silent corruption."""
        if name not in self._durable:
            raise FileNotFoundError(name)
        self._durable[name][byte_offset] ^= mask
        if name in self._cache and byte_offset < len(self._cache[name]):
            self._cache[name][byte_offset] ^= mask


class SegmentedWAL:
    """Production-style segmented binary WAL implementing the ``Storage`` protocol."""

    SNAP_FILE = "snapshot.bin"

    def __init__(
        self,
        fs: DiskFS,
        bootstrap: Snapshot,
        *,
        max_segment_bytes: int = 64 * 1024,
        sync: bool = True,
    ) -> None:
        if max_segment_bytes < 128:
            raise ValueError("max_segment_bytes must be >= 128")
        self.fs = fs
        self.max_segment_bytes = max_segment_bytes
        self.sync = sync
        self._bootstrap = bootstrap
        self.truncated_torn_bytes = 0
        self.segments_rotated = 0
        self.segments_garbage_collected = 0

        self._active_seq = 1
        self._seg_max_entry_index: dict[int, int] = {}
        hs, snap, entries = self._recover()
        self.hard_state = hs
        self.snapshot = snap
        self.entries = entries

    # --------------------------------------------------------------------- file naming
    @staticmethod
    def _seg_name(seq: int) -> str:
        return f"{seq:08d}.wal"

    @staticmethod
    def _parse_seq(name: str) -> int | None:
        if name.endswith(".wal") and len(name) == 12 and name[:8].isdigit():
            return int(name[:8])
        return None

    def segment_files(self) -> list[str]:
        seqs = [s for name in self.fs.list_files() if (s := self._parse_seq(name)) is not None]
        return [self._seg_name(s) for s in sorted(seqs)]

    # --------------------------------------------------------------------- Storage API
    def load(self) -> tuple[HardState, Snapshot, list[Entry]]:
        return self.hard_state, self.snapshot, list(self.entries)

    def save(self, rd: Ready) -> None:
        if rd.snapshot is not None:
            self.save_snapshot(rd.snapshot)
        bridge = get_helios_bridge()
        chunks: list[bytes] = []
        max_idx = self._seg_max_entry_index.get(self._active_seq, 0)

        if rd.entries:
            first_new = rd.entries[0].index
            if self.entries and first_new <= self.entries[-1].index:
                chunks.append(bridge.wal_encode_record(first_new, 0, _REC_TRUNCATE_FROM, b""))
                self.entries = [e for e in self.entries if e.index < first_new]
            for e in rd.entries:
                chunks.append(
                    bridge.wal_encode_record(
                        e.index, e.term, _KIND_TO_CODE[e.kind], e.data.encode("utf-8")
                    )
                )
                self.entries.append(e)
                max_idx = max(max_idx, e.index)

        if rd.hard_state is not None:
            self.hard_state = rd.hard_state
            hs = rd.hard_state
            vote_i64 = -1 if hs.vote is None else int(hs.vote)
            hs_bytes = _HS_STRUCT.pack(hs.term, vote_i64, hs.commit)
            chunks.append(bridge.wal_encode_record(0, hs.term, _REC_HARD_STATE, hs_bytes))

        if chunks:
            blob = b"".join(chunks)
            active_name = self._seg_name(self._active_seq)
            cur_size = self.fs.size(active_name)
            if cur_size > 0 and cur_size + len(blob) > self.max_segment_bytes:
                self._active_seq += 1
                self.segments_rotated += 1
                active_name = self._seg_name(self._active_seq)
            self.fs.append(active_name, blob, sync=self.sync)
            if max_idx > 0:
                self._seg_max_entry_index[self._active_seq] = max(
                    self._seg_max_entry_index.get(self._active_seq, 0), max_idx
                )

    def save_snapshot(self, snap: Snapshot) -> None:
        if snap.index <= self.snapshot.index:
            return
        bridge = get_helios_bridge()
        body = json.dumps(snapshot_to_json(snap), separators=(",", ":"), sort_keys=True).encode()
        frame = bridge.wal_encode_record(snap.index, snap.term, 0, body)
        self.fs.atomic_replace(self.SNAP_FILE, frame, sync=self.sync)
        self.snapshot = snap
        self.entries = [e for e in self.entries if e.index > snap.index]

        # Rotate to a fresh segment and garbage-collect sealed segments completely superseded.
        self._active_seq += 1
        self.segments_rotated += 1
        hs = self.hard_state
        vote_i64 = -1 if hs.vote is None else int(hs.vote)
        hs_frame = bridge.wal_encode_record(
            0, hs.term, _REC_HARD_STATE, _HS_STRUCT.pack(hs.term, vote_i64, hs.commit)
        )
        self.fs.append(self._seg_name(self._active_seq), hs_frame, sync=self.sync)

        for name in self.segment_files():
            seq = self._parse_seq(name)
            assert seq is not None
            if seq >= self._active_seq:
                continue
            seg_max = self._seg_max_entry_index.get(seq, 0)
            if seg_max <= snap.index:
                self.fs.remove(name)
                self._seg_max_entry_index.pop(seq, None)
                self.segments_garbage_collected += 1

    # --------------------------------------------------------------------- crash recovery
    def _recover(self) -> tuple[HardState, Snapshot, list[Entry]]:
        bridge = get_helios_bridge()
        snap = self._bootstrap
        if self.fs.exists(self.SNAP_FILE):
            s_buf = self.fs.read(self.SNAP_FILE)
            frames, valid, reason = bridge.wal_scan_frames(s_buf)
            if reason != WalStopReason.EOF_CLEAN or valid != len(s_buf) or len(frames) != 1:
                raise WalCorruptionError(f"corrupt snapshot image (reason={reason})")
            snap = snapshot_from_json(json.loads(frames[0].payload.decode("utf-8")))

        hs = HardState()
        entries: list[Entry] = []
        seg_names = self.segment_files()
        if not seg_names:
            self._active_seq = 1
            return hs, snap, entries

        for idx, name in enumerate(seg_names):
            seq = self._parse_seq(name)
            assert seq is not None
            is_last_seg = idx == len(seg_names) - 1
            raw = self.fs.read(name)
            frames, valid_bytes, reason = bridge.wal_scan_frames(raw)

            if reason in (WalStopReason.CRC_MISMATCH, WalStopReason.OVERSIZED):
                # A CRC mismatch at the very tail of the active segment can come from a
                # partially overwritten sector during a crash ONLY if no valid frames follow;
                # inside any earlier segment it is always silent data corruption.
                if not is_last_seg:
                    raise WalCorruptionError(
                        f"silent bitrot in sealed WAL segment {name} at byte {valid_bytes}"
                    )
                raise WalCorruptionError(
                    f"CRC32C mismatch in WAL segment {name} at byte {valid_bytes}"
                )

            if valid_bytes < len(raw):
                if not is_last_seg:
                    raise WalCorruptionError(
                        f"unexpected torn record in non-tail WAL segment {name}"
                    )
                self.truncated_torn_bytes += len(raw) - valid_bytes
                self.fs.truncate(name, valid_bytes, sync=self.sync)

            seg_max = 0
            for fr in frames:
                if fr.kind in _CODE_TO_KIND:
                    entry = Entry(
                        index=fr.index,
                        term=fr.term,
                        kind=_CODE_TO_KIND[fr.kind],
                        data=fr.payload.decode("utf-8"),
                    )
                    entries.append(entry)
                    seg_max = max(seg_max, entry.index)
                elif fr.kind == _REC_TRUNCATE_FROM:
                    entries = [e for e in entries if e.index < fr.index]
                elif fr.kind == _REC_HARD_STATE:
                    term, vote_i64, commit = _HS_STRUCT.unpack(fr.payload)
                    hs = HardState(
                        term=int(term),
                        vote=None if vote_i64 < 0 else int(vote_i64),
                        commit=int(commit),
                    )
            if seg_max > 0:
                self._seg_max_entry_index[seq] = seg_max
            self._active_seq = seq

        entries = [e for e in entries if e.index > snap.index]
        return hs, snap, entries
