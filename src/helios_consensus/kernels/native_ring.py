"""SHA-256 cached C++20 compiler & ctypes bridge for Google Jump Consistent Hash & CRC32C WAL."""

from __future__ import annotations

import ctypes
import hashlib
import platform
import shutil
import subprocess
from pathlib import Path

import numpy as np
import numpy.typing as npt

from helios_consensus.types import JumpHashWALReport

U64Array = npt.NDArray[np.uint64]
I32Array = npt.NDArray[np.int32]


def _py_jump_consistent_hash(key: int, num_buckets: int) -> int:
    if num_buckets < 1:
        raise ValueError("num_buckets must be >= 1")
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


class HeliosKernelBridge:
    """Compiles `csrc/helios_kernels.cpp` (`-O3 -std=c++20`) with bit-exact Python fallback."""

    def __init__(self, csrc_path: Path | None = None) -> None:
        repo_root = Path(__file__).resolve().parents[3]
        self._src = csrc_path or (repo_root / "csrc" / "helios_kernels.cpp")
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

        cache_dir = self._src.parent.parent / ".helios_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        src_hash = hashlib.sha256(self._src.read_bytes()).hexdigest()[:12]
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
        if self._lib is not None:
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
            ptr = arr.ctypes.data_as(ctypes.POINTER(ctypes.c_uint8))
            return int(self._lib.helios_crc32c(ptr, len(payload)))
        return _py_crc32c(payload)


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
    """Verify Google Jump Consistent Hash minimal-disruption theorem (`1 / B_after` keys moved),
    bit-exact C++20 vs Python parity, and Castagnoli CRC32C single-bit WAL corruption detection.
    """
    rng = np.random.default_rng(seed)
    keys = rng.integers(0, (1 << 63) - 1, size=num_keys, dtype=np.uint64)

    cpp_b = get_helios_bridge()
    py_b = HeliosKernelBridge(csrc_path=Path("/tmp/missing_helios_kernel.cpp"))

    b_old = cpp_b.jump_hash_batch(keys, buckets_before)
    b_new = cpp_b.jump_hash_batch(keys, buckets_after)

    # Verify bit-exact parity on 512 probe keys
    py_probe = py_b.jump_hash_batch(keys[:512], buckets_before)
    mismatches = int(np.sum(b_old[:512] != py_probe))

    moved_frac = float(np.mean(b_old != b_new))
    theo_frac = (buckets_after - buckets_before) / float(buckets_after)

    counts = np.bincount(b_old, minlength=buckets_before)
    ideal = num_keys / float(buckets_before)
    skew_pct = float(np.max(np.abs(counts - ideal)) / ideal) * 100.0

    # Verify 64 Raft Write-Ahead Log frames + single-bit corruption detection via CRC32C
    caught = 0
    for idx in range(64):
        frame = f"WAL|term={idx // 8}|index={idx}|cmd=SET shard_{idx}={idx * 17}".encode()
        digest = cpp_b.crc32c(frame)
        assert digest == py_b.crc32c(frame)
        corrupted = bytearray(frame)
        corrupted[idx % len(corrupted)] ^= 1 << (idx % 7)
        if cpp_b.crc32c(bytes(corrupted)) != digest:
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
