"""SHA-256 cached C++20 compiler & ctypes bridge for HeliosConsensus systems kernels.

Binds `src/helios_consensus/_csrc/helios_kernels.cpp` (`-O3 -std=c++20`) with bit-exact
pure-Python fallbacks across all 6 kernel families:
  1. Slicing-by-8 Castagnoli CRC32C (`0x82F63B78`)
  2. Lamping & Veach Jump Consistent Hash (scalar & batch)
  3. Binary Segmented WAL Record Encoder & Crash-Recovery Frame Scanner
  4. Wing & Gong + Lowe Bitmask Register Linearizability Solver (`N <= 62`)
  5. Single & Joint-Consensus Quorum Commit Watermark
  6. FIPS 180-4 SHA-256 & RFC-6962 Binary Merkle Tree Root
"""

from __future__ import annotations

import ctypes
import hashlib
import platform
import shutil
import struct
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import numpy.typing as npt

from helios_consensus.types import JumpHashWALReport

if TYPE_CHECKING:
    from helios_consensus.check.linearizability import Operation

U64Array = npt.NDArray[np.uint64]
I32Array = npt.NDArray[np.int32]

_WAL_HEADER = struct.Struct("<IIQQB")  # payload_len, crc32c, index, term, kind (25 bytes)


class WalStopReason:
    EOF_CLEAN = 0
    TORN_HEADER = 1
    TORN_PAYLOAD = 2
    CRC_MISMATCH = 3
    OVERSIZED = 4


@dataclass(frozen=True, slots=True)
class ScannedWalFrame:
    index: int
    term: int
    kind: int
    payload: bytes


def _py_jump_consistent_hash(key: int, num_buckets: int) -> int:
    if num_buckets < 1:
        raise ValueError("num_buckets must be >= 1")
    if num_buckets == 1:
        return 0
    k = key & 0xFFFFFFFFFFFFFFFF
    b = -1
    j = 0
    while j < num_buckets:
        b = j
        k = ((k * 2862933555777941757) + 1) & 0xFFFFFFFFFFFFFFFF
        j = int((b + 1) * (float(1 << 31) / float((k >> 33) + 1)))
    return b


def _py_crc32c(data: bytes) -> int:
    crc = 0xFFFFFFFF
    for byte_val in data:
        crc ^= byte_val
        for _ in range(8):
            mask = -(crc & 1) & 0xFFFFFFFF
            crc = (crc >> 1) ^ (0x82F63B78 & mask)
    return (~crc) & 0xFFFFFFFF


def _py_wal_encode_record(index: int, term: int, kind: int, payload: bytes) -> bytes:
    inner = struct.pack("<QQB", index, term, kind) + payload
    crc = _py_crc32c(inner)
    return struct.pack("<II", len(payload), crc) + inner


def _py_wal_scan_frames(
    buf: bytes, max_payload_len: int = 16 * 1024 * 1024
) -> tuple[list[ScannedWalFrame], int, int]:
    frames: list[ScannedWalFrame] = []
    pos = 0
    n = len(buf)
    reason = WalStopReason.EOF_CLEAN
    while pos < n:
        if n - pos < _WAL_HEADER.size:
            reason = WalStopReason.TORN_HEADER
            break
        plen, expected_crc, index, term, kind = _WAL_HEADER.unpack_from(buf, pos)
        if plen > max_payload_len:
            reason = WalStopReason.OVERSIZED
            break
        end = pos + _WAL_HEADER.size + plen
        if end > n:
            reason = WalStopReason.TORN_PAYLOAD
            break
        actual_crc = _py_crc32c(buf[pos + 8 : end])
        if actual_crc != expected_crc:
            reason = WalStopReason.CRC_MISMATCH
            break
        frames.append(
            ScannedWalFrame(
                index=index,
                term=term,
                kind=kind,
                payload=buf[pos + _WAL_HEADER.size : end],
            )
        )
        pos = end
    return frames, pos, reason


def _py_quorum_watermark(matches: list[int]) -> int:
    if not matches:
        return 0
    ordered = sorted(matches, reverse=True)
    return ordered[(len(ordered) // 2)]


def _py_joint_commit_index(primary: list[int], secondary: list[int] | None = None) -> int:
    w1 = _py_quorum_watermark(primary)
    if not secondary:
        return w1
    return min(w1, _py_quorum_watermark(secondary))


def _py_merkle_root_rfc6962(leaves: list[bytes]) -> bytes:
    if not leaves:
        return hashlib.sha256(b"").digest()
    level = [hashlib.sha256(b"\x00" + leaf).digest() for leaf in leaves]
    while len(level) > 1:
        nxt: list[bytes] = []
        for i in range(0, len(level), 2):
            if i + 1 < len(level):
                nxt.append(hashlib.sha256(b"\x01" + level[i] + level[i + 1]).digest())
            else:
                nxt.append(level[i])
        level = nxt
    return level[0]


class HeliosKernelBridge:
    """Compiles `_csrc/helios_kernels.cpp` (`-O3 -std=c++20`) with bit-exact Python fallback."""

    def __init__(self, csrc_path: Path | None = None) -> None:
        pkg_dir = Path(__file__).resolve().parents[1]
        self._src = csrc_path or (pkg_dir / "_csrc" / "helios_kernels.cpp")
        self._lib: ctypes.CDLL | None = None
        self._backend = "python-fallback"
        self._try_compile_and_load()

    @property
    def backend(self) -> str:
        return self._backend

    @property
    def is_native_cpp(self) -> bool:
        return self._lib is not None

    def _try_compile_and_load(self) -> None:
        if not self._src.exists():
            return
        compiler = shutil.which("clang++") or shutil.which("g++")
        if compiler is None:
            return

        cache_dir = Path(tempfile.gettempdir()) / "helios_consensus_native_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        src_hash = hashlib.sha256(self._src.read_bytes()).hexdigest()[:16]
        ext = "dylib" if platform.system() == "Darwin" else "so"
        out_lib = cache_dir / f"libhelios_kernels_{src_hash}.{ext}"

        if not out_lib.exists():
            cmd = [
                compiler,
                "-O3",
                "-std=c++20",
                "-shared",
                "-fPIC",
                str(self._src),
                "-o",
                str(out_lib),
            ]
            proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
            if proc.returncode != 0:
                return

        lib = ctypes.CDLL(str(out_lib))
        lib.helios_jump_consistent_hash.argtypes = [ctypes.c_uint64, ctypes.c_int32]
        lib.helios_jump_consistent_hash.restype = ctypes.c_int32

        lib.helios_jump_hash_batch.argtypes = [
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.c_int32,
            ctypes.c_int32,
            ctypes.POINTER(ctypes.c_int32),
        ]
        lib.helios_jump_hash_batch.restype = None

        lib.helios_crc32c.argtypes = [ctypes.POINTER(ctypes.c_uint8), ctypes.c_int32]
        lib.helios_crc32c.restype = ctypes.c_uint32

        lib.helios_wal_encode_record.argtypes = [
            ctypes.c_uint64,
            ctypes.c_uint64,
            ctypes.c_uint8,
            ctypes.POINTER(ctypes.c_uint8),
            ctypes.c_int32,
            ctypes.POINTER(ctypes.c_uint8),
            ctypes.c_int32,
        ]
        lib.helios_wal_encode_record.restype = ctypes.c_int32

        lib.helios_wal_scan_frames.argtypes = [
            ctypes.POINTER(ctypes.c_uint8),
            ctypes.c_int32,
            ctypes.c_int32,
            ctypes.c_int32,
            ctypes.POINTER(ctypes.c_int32),
            ctypes.POINTER(ctypes.c_int32),
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_uint64),
            ctypes.POINTER(ctypes.c_uint8),
            ctypes.POINTER(ctypes.c_int32),
            ctypes.POINTER(ctypes.c_int32),
        ]
        lib.helios_wal_scan_frames.restype = ctypes.c_int32

        lib.helios_wgl_register.argtypes = [
            ctypes.c_int32,
            ctypes.POINTER(ctypes.c_int32),
            ctypes.POINTER(ctypes.c_int32),
            ctypes.POINTER(ctypes.c_int8),
            ctypes.POINTER(ctypes.c_int64),
            ctypes.POINTER(ctypes.c_int8),
            ctypes.POINTER(ctypes.c_int64),
            ctypes.POINTER(ctypes.c_int32),
            ctypes.POINTER(ctypes.c_int64),
            ctypes.POINTER(ctypes.c_int8),
            ctypes.POINTER(ctypes.c_int64),
            ctypes.c_int32,
            ctypes.POINTER(ctypes.c_int32),
        ]
        lib.helios_wgl_register.restype = ctypes.c_int32

        lib.helios_joint_commit_index.argtypes = [
            ctypes.POINTER(ctypes.c_int64),
            ctypes.c_int32,
            ctypes.POINTER(ctypes.c_int64),
            ctypes.c_int32,
        ]
        lib.helios_joint_commit_index.restype = ctypes.c_int64

        lib.helios_sha256.argtypes = [
            ctypes.POINTER(ctypes.c_uint8),
            ctypes.c_int32,
            ctypes.POINTER(ctypes.c_uint8),
        ]
        lib.helios_sha256.restype = None

        lib.helios_merkle_root_rfc6962.argtypes = [
            ctypes.POINTER(ctypes.c_uint8),
            ctypes.POINTER(ctypes.c_int32),
            ctypes.POINTER(ctypes.c_int32),
            ctypes.c_int32,
            ctypes.POINTER(ctypes.c_uint8),
        ]
        lib.helios_merkle_root_rfc6962.restype = None

        self._lib = lib
        self._backend = f"c++20-native ({Path(compiler).name} -O3)"

    def jump_hash(self, key: int, num_buckets: int) -> int:
        if num_buckets < 1:
            raise ValueError("num_buckets must be >= 1")
        if self._lib is not None:
            return int(
                self._lib.helios_jump_consistent_hash(
                    ctypes.c_uint64(key & 0xFFFFFFFFFFFFFFFF), ctypes.c_int32(num_buckets)
                )
            )
        return _py_jump_consistent_hash(key, num_buckets)

    def jump_hash_batch(self, keys: U64Array, num_buckets: int) -> I32Array:
        if num_buckets < 1:
            raise ValueError("num_buckets must be >= 1")
        k_arr = np.ascontiguousarray(keys, dtype=np.uint64)
        count = int(k_arr.shape[0])
        out = np.empty(count, dtype=np.int32)
        if self._lib is not None and count > 0:
            self._lib.helios_jump_hash_batch(
                k_arr.ctypes.data_as(ctypes.POINTER(ctypes.c_uint64)),
                count,
                num_buckets,
                out.ctypes.data_as(ctypes.POINTER(ctypes.c_int32)),
            )
            return out
        for i in range(count):
            out[i] = _py_jump_consistent_hash(int(k_arr[i]), num_buckets)
        return out

    def crc32c(self, payload: bytes) -> int:
        if self._lib is not None:
            arr = np.frombuffer(payload, dtype=np.uint8)
            ptr = (
                arr.ctypes.data_as(ctypes.POINTER(ctypes.c_uint8))
                if len(payload) > 0
                else ctypes.POINTER(ctypes.c_uint8)()
            )
            return int(self._lib.helios_crc32c(ptr, len(payload)))
        return _py_crc32c(payload)

    def wal_encode_record(self, index: int, term: int, kind: int, payload: bytes) -> bytes:
        if self._lib is None:
            return _py_wal_encode_record(index, term, kind, payload)
        cap = 25 + len(payload)
        out = (ctypes.c_uint8 * cap)()
        in_arr = np.frombuffer(payload, dtype=np.uint8)
        in_ptr = (
            in_arr.ctypes.data_as(ctypes.POINTER(ctypes.c_uint8))
            if len(payload) > 0
            else ctypes.POINTER(ctypes.c_uint8)()
        )
        written = int(
            self._lib.helios_wal_encode_record(
                ctypes.c_uint64(index),
                ctypes.c_uint64(term),
                ctypes.c_uint8(kind),
                in_ptr,
                len(payload),
                out,
                cap,
            )
        )
        if written < 0:
            raise ValueError("failed to encode binary WAL frame")
        return bytes(out[:written])

    def wal_scan_frames(
        self, buf: bytes, max_payload_len: int = 16 * 1024 * 1024
    ) -> tuple[list[ScannedWalFrame], int, int]:
        if self._lib is None or not buf:
            return _py_wal_scan_frames(buf, max_payload_len=max_payload_len)
        n = len(buf)
        max_frames = max(1, (n // 25) + 1)
        in_arr = np.frombuffer(buf, dtype=np.uint8)
        offs = (ctypes.c_int32 * max_frames)()
        lens = (ctypes.c_int32 * max_frames)()
        idxs = (ctypes.c_uint64 * max_frames)()
        terms = (ctypes.c_uint64 * max_frames)()
        kinds = (ctypes.c_uint8 * max_frames)()
        valid_bytes = ctypes.c_int32(0)
        stop_reason = ctypes.c_int32(0)

        cnt = int(
            self._lib.helios_wal_scan_frames(
                in_arr.ctypes.data_as(ctypes.POINTER(ctypes.c_uint8)),
                n,
                max_payload_len,
                max_frames,
                offs,
                lens,
                idxs,
                terms,
                kinds,
                ctypes.byref(valid_bytes),
                ctypes.byref(stop_reason),
            )
        )
        frames = [
            ScannedWalFrame(
                index=int(idxs[i]),
                term=int(terms[i]),
                kind=int(kinds[i]),
                payload=buf[int(offs[i]) : int(offs[i]) + int(lens[i])],
            )
            for i in range(cnt)
        ]
        return frames, int(valid_bytes.value), int(stop_reason.value)

    def wgl_check_register(
        self, ops: list[Operation], budget: int = 2_000_000
    ) -> tuple[bool, int, bool] | None:
        """Solve register linearizability in C++20 when ``len(ops) <= 62``; None to fall back."""
        if self._lib is None or len(ops) > 62:
            return None
        n = len(ops)
        if n == 0:
            return True, 0, False
        kind_code = {"read": 0, "write": 1, "cas": 2}
        c_ids = (ctypes.c_int32 * n)(*(op.op_id for op in ops))
        c_kinds = (ctypes.c_int32 * n)(*(kind_code[op.kind.value] for op in ops))
        c_has_v = (ctypes.c_int8 * n)(*(0 if op.value is None else 1 for op in ops))
        c_vals = (ctypes.c_int64 * n)(*(0 if op.value is None else int(op.value) for op in ops))
        c_has_e = (ctypes.c_int8 * n)(*(0 if op.expected is None else 1 for op in ops))
        c_exps = (ctypes.c_int64 * n)(
            *(0 if op.expected is None else int(op.expected) for op in ops)
        )
        c_oks = (ctypes.c_int32 * n)(*(-1 if op.ok is None else (1 if op.ok else 0) for op in ops))
        c_calls = (ctypes.c_int64 * n)(*(int(op.call) for op in ops))
        c_has_r = (ctypes.c_int8 * n)(*(0 if op.ret is None else 1 for op in ops))
        c_rets = (ctypes.c_int64 * n)(*(0 if op.ret is None else int(op.ret) for op in ops))
        explored = ctypes.c_int32(0)

        rc = int(
            self._lib.helios_wgl_register(
                n,
                c_ids,
                c_kinds,
                c_has_v,
                c_vals,
                c_has_e,
                c_exps,
                c_oks,
                c_calls,
                c_has_r,
                c_rets,
                int(budget),
                ctypes.byref(explored),
            )
        )
        if rc < 0:
            return None
        if rc == 2:
            return True, int(explored.value), True
        return rc == 1, int(explored.value), False

    def joint_commit_index(self, primary: list[int], secondary: list[int] | None = None) -> int:
        if self._lib is None or not primary:
            return _py_joint_commit_index(primary, secondary)
        p_arr = (ctypes.c_int64 * len(primary))(*(int(x) for x in primary))
        if secondary:
            s_arr = (ctypes.c_int64 * len(secondary))(*(int(x) for x in secondary))
            return int(
                self._lib.helios_joint_commit_index(p_arr, len(primary), s_arr, len(secondary))
            )
        return int(
            self._lib.helios_joint_commit_index(
                p_arr, len(primary), ctypes.POINTER(ctypes.c_int64)(), 0
            )
        )

    def sha256(self, data: bytes) -> bytes:
        if self._lib is None:
            return hashlib.sha256(data).digest()
        out = (ctypes.c_uint8 * 32)()
        arr = np.frombuffer(data, dtype=np.uint8)
        ptr = (
            arr.ctypes.data_as(ctypes.POINTER(ctypes.c_uint8))
            if len(data) > 0
            else ctypes.POINTER(ctypes.c_uint8)()
        )
        self._lib.helios_sha256(ptr, len(data), out)
        return bytes(out)

    def merkle_root_rfc6962(self, leaves: list[bytes]) -> bytes:
        if self._lib is None:
            return _py_merkle_root_rfc6962(leaves)
        out = (ctypes.c_uint8 * 32)()
        n = len(leaves)
        if n == 0:
            self._lib.helios_merkle_root_rfc6962(
                ctypes.POINTER(ctypes.c_uint8)(),
                ctypes.POINTER(ctypes.c_int32)(),
                ctypes.POINTER(ctypes.c_int32)(),
                0,
                out,
            )
            return bytes(out)
        offs = (ctypes.c_int32 * n)()
        lens = (ctypes.c_int32 * n)()
        pos = 0
        for i, leaf in enumerate(leaves):
            offs[i] = pos
            lens[i] = len(leaf)
            pos += len(leaf)
        joined = b"".join(leaves)
        arr = np.frombuffer(joined, dtype=np.uint8)
        ptr = (
            arr.ctypes.data_as(ctypes.POINTER(ctypes.c_uint8))
            if len(joined) > 0
            else ctypes.POINTER(ctypes.c_uint8)()
        )
        self._lib.helios_merkle_root_rfc6962(ptr, offs, lens, n, out)
        return bytes(out)


_GLOBAL_BRIDGE: HeliosKernelBridge | None = None


def get_helios_bridge() -> HeliosKernelBridge:
    global _GLOBAL_BRIDGE
    if _GLOBAL_BRIDGE is None:
        _GLOBAL_BRIDGE = HeliosKernelBridge()
    return _GLOBAL_BRIDGE


def benchmark_jump_hash_and_crc32c(
    *,
    num_keys: int = 20_000,
    buckets_before: int = 12,
    buckets_after: int = 13,
    seed: int = 101,
) -> JumpHashWALReport:
    """Verify Google Jump Consistent Hash minimal-disruption property (`1 / B_after` keys moved),
    bit-exact C++20 vs Python parity, and Castagnoli CRC32C single-bit WAL corruption detection.
    """
    rng = np.random.default_rng(seed)
    keys = rng.integers(0, (1 << 63) - 1, size=num_keys, dtype=np.uint64)

    cpp_b = get_helios_bridge()
    py_b = HeliosKernelBridge(csrc_path=Path("/tmp/missing_helios_kernel.cpp"))

    b_old = cpp_b.jump_hash_batch(keys, buckets_before)
    b_new = cpp_b.jump_hash_batch(keys, buckets_after)

    py_probe = py_b.jump_hash_batch(keys[:512], buckets_before)
    mismatches = int(np.sum(b_old[:512] != py_probe))

    moved_frac = float(np.mean(b_old != b_new))
    theo_frac = (buckets_after - buckets_before) / float(buckets_after)

    counts = np.bincount(b_old, minlength=buckets_before)
    ideal = num_keys / float(buckets_before)
    skew_pct = float(np.max(np.abs(counts - ideal)) / ideal) * 100.0

    caught = 0
    for idx in range(64):
        frame = cpp_b.wal_encode_record(
            index=idx + 1,
            term=(idx // 8) + 1,
            kind=0,
            payload=f"SET shard_{idx}={idx * 17}".encode(),
        )
        scanned, _, reason = cpp_b.wal_scan_frames(frame)
        assert len(scanned) == 1 and reason == WalStopReason.EOF_CLEAN
        assert frame == py_b.wal_encode_record(
            index=idx + 1,
            term=(idx // 8) + 1,
            kind=0,
            payload=f"SET shard_{idx}={idx * 17}".encode(),
        )
        corrupted = bytearray(frame)
        corrupted[8 + (idx % (len(corrupted) - 8))] ^= 1 << (idx % 7)
        _, _, bad_reason = cpp_b.wal_scan_frames(bytes(corrupted))
        if bad_reason == WalStopReason.CRC_MISMATCH:
            caught += 1

    return JumpHashWALReport(
        kernel_backend=cpp_b.backend,
        keys_routed=num_keys,
        buckets_before=buckets_before,
        buckets_after=buckets_after,
        theoretical_remapped_fraction=round(theo_frac, 5),
        measured_remapped_fraction=round(moved_frac, 5),
        max_bucket_load_skew_pct=round(skew_pct, 2),
        cpp_vs_python_hash_mismatches=mismatches,
        wal_frames_verified=64,
        bitflip_corruptions_caught=caught,
    )
