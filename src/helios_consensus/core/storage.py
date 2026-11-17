"""Stable storage for Raft: an in-memory store and a crash-safe file WAL.

The file WAL frames every record as ``[u32 length][u32 crc32c][json payload]``. On
recovery it replays records until the first torn or corrupt frame and truncates the file
there, which is the standard way to survive a crash in the middle of a write.
"""

from __future__ import annotations

import json
import os
import struct
from pathlib import Path
from typing import Any, Protocol

from helios_consensus.core.messages import (
    Entry,
    EntryKind,
    HardState,
    Membership,
    Ready,
    Snapshot,
)
from helios_consensus.kernels.native_ring import get_helios_bridge

_HEADER = struct.Struct("<II")
_MAX_RECORD = 64 * 1024 * 1024


class Storage(Protocol):
    def save(self, rd: Ready) -> None: ...

    def save_snapshot(self, snap: Snapshot) -> None: ...

    def load(self) -> tuple[HardState, Snapshot, list[Entry]]: ...


def _merge_entries(existing: list[Entry], new: list[Entry]) -> list[Entry]:
    if not new:
        return existing
    first = new[0].index
    return [e for e in existing if e.index < first] + list(new)


class MemoryStorage:
    """Stable storage that survives simulated crashes because it lives outside the node."""

    def __init__(self, bootstrap: Snapshot) -> None:
        self.hard_state = HardState()
        self.snapshot = bootstrap
        self.entries: list[Entry] = []
        self.writes = 0

    def save(self, rd: Ready) -> None:
        if rd.snapshot is not None:
            self.save_snapshot(rd.snapshot)
        if rd.entries:
            self.entries = _merge_entries(self.entries, rd.entries)
        if rd.hard_state is not None:
            self.hard_state = rd.hard_state
        if rd.snapshot is not None or rd.entries or rd.hard_state is not None:
            self.writes += 1

    def save_snapshot(self, snap: Snapshot) -> None:
        if snap.index <= self.snapshot.index:
            return
        self.snapshot = snap
        self.entries = [e for e in self.entries if e.index > snap.index]

    def load(self) -> tuple[HardState, Snapshot, list[Entry]]:
        return self.hard_state, self.snapshot, list(self.entries)


# ----------------------------------------------------------------------------- encoding
def _membership_to_json(m: Membership) -> dict[str, list[int]]:
    return {"voters": sorted(m.voters), "learners": sorted(m.learners)}


def _membership_from_json(raw: dict[str, Any]) -> Membership:
    return Membership(voters=frozenset(raw["voters"]), learners=frozenset(raw["learners"]))


def snapshot_to_json(s: Snapshot) -> dict[str, Any]:
    return {
        "index": s.index,
        "term": s.term,
        "membership": _membership_to_json(s.membership),
        "data": s.data,
    }


def snapshot_from_json(raw: dict[str, Any]) -> Snapshot:
    return Snapshot(
        index=int(raw["index"]),
        term=int(raw["term"]),
        membership=_membership_from_json(raw["membership"]),
        data=str(raw["data"]),
    )


def _entry_to_json(e: Entry) -> dict[str, Any]:
    return {"i": e.index, "t": e.term, "k": e.kind.value, "d": e.data}


def _entry_from_json(raw: dict[str, Any]) -> Entry:
    return Entry(index=int(raw["i"]), term=int(raw["t"]), kind=EntryKind(raw["k"]), data=raw["d"])


def encode_frame(payload: dict[str, Any]) -> bytes:
    body = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    return _HEADER.pack(len(body), get_helios_bridge().crc32c(body)) + body


def decode_frames(buf: bytes) -> tuple[list[dict[str, Any]], int]:
    """Decode frames; returns (records, number of valid bytes)."""
    records: list[dict[str, Any]] = []
    pos = 0
    crc = get_helios_bridge().crc32c
    while pos + _HEADER.size <= len(buf):
        length, checksum = _HEADER.unpack_from(buf, pos)
        start = pos + _HEADER.size
        end = start + length
        if length > _MAX_RECORD or end > len(buf):
            break
        body = buf[start:end]
        if crc(body) != checksum:
            break
        try:
            records.append(json.loads(body))
        except ValueError:
            break
        pos = end
    return records, pos


class FileStorage:
    """Directory-backed WAL + snapshot file. ``fsync`` can be disabled for fast tests."""

    WAL = "wal.log"
    SNAP = "snapshot.json"

    def __init__(self, directory: Path, bootstrap: Snapshot, *, fsync: bool = True) -> None:
        self.dir = directory
        self.dir.mkdir(parents=True, exist_ok=True)
        self.fsync = fsync
        self.recovered_torn_bytes = 0
        self._bootstrap = bootstrap
        hs, snap, entries = self._recover()
        self.hard_state, self.snapshot, self.entries = hs, snap, entries

    # ----------------------------------------------------------------------------- API
    def save(self, rd: Ready) -> None:
        if rd.snapshot is not None:
            self.save_snapshot(rd.snapshot)
        frames: list[bytes] = []
        if rd.entries:
            self.entries = _merge_entries(self.entries, rd.entries)
            frames.append(
                encode_frame({"type": "ents", "ents": [_entry_to_json(e) for e in rd.entries]})
            )
        if rd.hard_state is not None:
            self.hard_state = rd.hard_state
            hs = rd.hard_state
            frames.append(
                encode_frame({"type": "hs", "term": hs.term, "vote": hs.vote, "commit": hs.commit})
            )
        if frames:
            self._append(b"".join(frames))

    def save_snapshot(self, snap: Snapshot) -> None:
        if snap.index <= self.snapshot.index:
            return
        self._atomic_write(self.dir / self.SNAP, json.dumps(snapshot_to_json(snap)).encode())
        self.snapshot = snap
        self.entries = [e for e in self.entries if e.index > snap.index]
        self._rewrite_wal()

    def load(self) -> tuple[HardState, Snapshot, list[Entry]]:
        return self.hard_state, self.snapshot, list(self.entries)

    # ----------------------------------------------------------------------------- internals
    def _recover(self) -> tuple[HardState, Snapshot, list[Entry]]:
        snap = self._bootstrap
        snap_path = self.dir / self.SNAP
        if snap_path.exists():
            snap = snapshot_from_json(json.loads(snap_path.read_text()))
        hs = HardState()
        entries: list[Entry] = []
        wal = self.dir / self.WAL
        if wal.exists():
            buf = wal.read_bytes()
            records, valid = decode_frames(buf)
            if valid < len(buf):
                self.recovered_torn_bytes = len(buf) - valid
                with wal.open("r+b") as fh:
                    fh.truncate(valid)
                    self._sync(fh)
            for rec in records:
                if rec["type"] == "ents":
                    entries = _merge_entries(entries, [_entry_from_json(r) for r in rec["ents"]])
                elif rec["type"] == "hs":
                    hs = HardState(term=rec["term"], vote=rec["vote"], commit=rec["commit"])
        entries = [e for e in entries if e.index > snap.index]
        return hs, snap, entries

    def _append(self, data: bytes) -> None:
        with (self.dir / self.WAL).open("ab") as fh:
            fh.write(data)
            self._sync(fh)

    def _rewrite_wal(self) -> None:
        hs = self.hard_state
        body = b""
        if self.entries:
            body += encode_frame(
                {"type": "ents", "ents": [_entry_to_json(e) for e in self.entries]}
            )
        body += encode_frame({"type": "hs", "term": hs.term, "vote": hs.vote, "commit": hs.commit})
        self._atomic_write(self.dir / self.WAL, body)

    def _atomic_write(self, path: Path, data: bytes) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        with tmp.open("wb") as fh:
            fh.write(data)
            self._sync(fh)
        os.replace(tmp, path)

    def _sync(self, fh: Any) -> None:
        if self.fsync:
            fh.flush()
            os.fsync(fh.fileno())
