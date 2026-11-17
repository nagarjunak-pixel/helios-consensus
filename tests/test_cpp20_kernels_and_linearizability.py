"""Differential Hypothesis tests for C++20 kernels and Wing-Gong-Lowe linearizability."""

from __future__ import annotations

import struct
from pathlib import Path

import numpy as np
from hypothesis import given, settings
from hypothesis import strategies as st

from helios_consensus.check.linearizability import (
    Operation,
    OpKind,
    check_history,
    check_register,
    python_check_register,
)
from helios_consensus.kernels.native_ring import (
    HeliosKernelBridge,
    WalStopReason,
    get_helios_bridge,
)


def _py_bridge() -> HeliosKernelBridge:
    return HeliosKernelBridge(csrc_path=Path("/tmp/nonexistent_helios_csrc.cpp"))


@settings(max_examples=60, deadline=None)
@given(payload=st.binary(min_size=0, max_size=260))
def test_cpp20_vs_python_crc32c_and_sha256_parity(payload: bytes) -> None:
    cpp_b = get_helios_bridge()
    py_b = _py_bridge()
    assert cpp_b.is_native_cpp is True
    assert py_b.is_native_cpp is False
    assert cpp_b.crc32c(payload) == py_b.crc32c(payload)
    assert cpp_b.sha256(payload) == py_b.sha256(payload)


@settings(max_examples=45, deadline=None)
@given(leaves=st.lists(st.binary(min_size=0, max_size=80), min_size=0, max_size=19))
def test_cpp20_vs_python_merkle_root_rfc6962_parity(leaves: list[bytes]) -> None:
    cpp_b = get_helios_bridge()
    py_b = _py_bridge()
    assert cpp_b.merkle_root_rfc6962(leaves) == py_b.merkle_root_rfc6962(leaves)


@settings(max_examples=45, deadline=None)
@given(
    primary=st.lists(st.integers(min_value=0, max_value=10_000), min_size=0, max_size=9),
    secondary=st.one_of(
        st.none(),
        st.lists(st.integers(min_value=0, max_value=10_000), min_size=0, max_size=9),
    ),
)
def test_cpp20_vs_python_joint_commit_watermark_parity(
    primary: list[int], secondary: list[int] | None
) -> None:
    cpp_b = get_helios_bridge()
    py_b = _py_bridge()
    assert cpp_b.joint_commit_index(primary, secondary) == py_b.joint_commit_index(
        primary, secondary
    )


@settings(max_examples=40, deadline=None)
@given(
    records=st.lists(
        st.tuples(
            st.integers(min_value=0, max_value=1_000_000),
            st.integers(min_value=0, max_value=10_000),
            st.integers(min_value=0, max_value=3),
            st.binary(min_size=0, max_size=96),
        ),
        min_size=1,
        max_size=12,
    )
)
def test_cpp20_vs_python_wal_encode_and_scan_parity(
    records: list[tuple[int, int, int, bytes]],
) -> None:
    cpp_b = get_helios_bridge()
    py_b = _py_bridge()

    cpp_frames = b"".join(cpp_b.wal_encode_record(i, t, k, p) for i, t, k, p in records)
    py_frames = b"".join(py_b.wal_encode_record(i, t, k, p) for i, t, k, p in records)
    assert cpp_frames == py_frames

    s_cpp, vb_cpp, r_cpp = cpp_b.wal_scan_frames(cpp_frames)
    s_py, vb_py, r_py = py_b.wal_scan_frames(py_frames)
    assert s_cpp == s_py
    assert vb_cpp == vb_py == len(cpp_frames)
    assert r_cpp == r_py == WalStopReason.EOF_CLEAN


def test_wal_scanner_stop_reasons_and_jump_hash_edge_cases() -> None:
    cpp_b = get_helios_bridge()
    py_b = _py_bridge()

    assert cpp_b.jump_hash(999, 1) == py_b.jump_hash(999, 1) == 0
    empty = np.array([], dtype=np.uint64)
    assert cpp_b.jump_hash_batch(empty, 4).shape == (0,)
    assert py_b.jump_hash_batch(empty, 4).shape == (0,)

    f1 = cpp_b.wal_encode_record(1, 1, 1, b"hello-raft")
    f2 = cpp_b.wal_encode_record(2, 1, 1, b"second-entry")

    # Empty buffer
    assert cpp_b.wal_scan_frames(b"") == ([], 0, WalStopReason.EOF_CLEAN)
    assert py_b.wal_scan_frames(b"") == ([], 0, WalStopReason.EOF_CLEAN)

    # Torn header (< 25 bytes after valid frame)
    torn_hdr = f1 + f2[:11]
    assert cpp_b.wal_scan_frames(torn_hdr)[1:] == (len(f1), WalStopReason.TORN_HEADER)
    assert py_b.wal_scan_frames(torn_hdr)[1:] == (len(f1), WalStopReason.TORN_HEADER)

    # Torn payload (header intact, payload cut short)
    torn_pay = f1 + f2[:-3]
    assert cpp_b.wal_scan_frames(torn_pay)[1:] == (len(f1), WalStopReason.TORN_PAYLOAD)
    assert py_b.wal_scan_frames(torn_pay)[1:] == (len(f1), WalStopReason.TORN_PAYLOAD)

    # Oversized payload (> max_payload_len)
    oversized = f1 + struct.pack("<IIQQB", 500, 0, 3, 1, 1)
    assert cpp_b.wal_scan_frames(oversized, max_payload_len=128)[1:] == (
        len(f1),
        WalStopReason.OVERSIZED,
    )
    assert py_b.wal_scan_frames(oversized, max_payload_len=128)[1:] == (
        len(f1),
        WalStopReason.OVERSIZED,
    )

    # Bitrot CRC mismatch in second frame
    bad = bytearray(f1 + f2)
    bad[len(f1) + 26] ^= 0x40
    assert cpp_b.wal_scan_frames(bytes(bad))[1:] == (len(f1), WalStopReason.CRC_MISMATCH)
    assert py_b.wal_scan_frames(bytes(bad))[1:] == (len(f1), WalStopReason.CRC_MISMATCH)


@st.composite
def _register_history_strategy(draw: st.DrawFn) -> list[Operation]:
    n = draw(st.integers(min_value=1, max_value=14))
    ops: list[Operation] = []
    for idx in range(n):
        kind = draw(st.sampled_from([OpKind.WRITE, OpKind.READ, OpKind.CAS]))
        call = draw(st.integers(min_value=0, max_value=20))
        indet = draw(st.booleans()) if kind is not OpKind.READ else False
        ret = None if indet else call + draw(st.integers(min_value=0, max_value=8))
        val = draw(st.one_of(st.none(), st.integers(min_value=0, max_value=3)))
        if kind is OpKind.WRITE and val is None:
            val = 0
        exp = (
            draw(st.one_of(st.none(), st.integers(min_value=0, max_value=3)))
            if kind is OpKind.CAS
            else None
        )
        ok = None if (kind is not OpKind.CAS or indet) else draw(st.booleans())
        ops.append(
            Operation(
                op_id=idx,
                process=idx % 4,
                kind=kind,
                key="k0",
                value=val,
                call=call,
                ret=ret,
                expected=exp,
                ok=ok,
            )
        )
    return ops


@settings(max_examples=70, deadline=None)
@given(ops=_register_history_strategy())
def test_cpp20_vs_python_wgl_linearizability_parity(ops: list[Operation]) -> None:
    cpp_ok, cpp_exp, cpp_exh = check_register(ops, force_python=False)
    py_ok, py_exp, py_exh = python_check_register(ops)
    assert cpp_ok == py_ok
    assert cpp_exh == py_exh is False
    assert cpp_exp == py_exp


def test_wgl_linearizability_known_histories_fallback_and_budget() -> None:
    assert check_register([]) == (True, 0, False)
    assert get_helios_bridge().wgl_check_register([]) == (True, 0, False)

    # Stale read after strictly completed write -> non-linearizable
    stale = [
        Operation(op_id=0, process=1, kind=OpKind.WRITE, key="x", value=1, call=0, ret=2),
        Operation(op_id=1, process=2, kind=OpKind.WRITE, key="x", value=2, call=3, ret=5),
        Operation(op_id=2, process=3, kind=OpKind.READ, key="x", value=1, call=6, ret=8),
        Operation(op_id=3, process=1, kind=OpKind.WRITE, key="y", value=9, call=0, ret=2),
        Operation(op_id=4, process=2, kind=OpKind.READ, key="y", value=9, call=3, ret=4),
    ]
    rep = check_history(stale)
    assert rep.ok is False
    assert rep.failing_keys == ["x"]
    assert rep.operations == 5
    assert rep.states_explored > 0

    # > 62 operations on a single key transparently falls back to arbitrary-precision Python WGL
    long_ops: list[Operation] = []
    for idx in range(65):
        long_ops.append(
            Operation(
                op_id=idx,
                process=idx % 3,
                kind=OpKind.WRITE if idx % 2 == 0 else OpKind.READ,
                key="big",
                value=idx // 2,
                call=idx * 2,
                ret=idx * 2 + 1,
            )
        )
    assert get_helios_bridge().wgl_check_register(long_ops) is None
    ok_long, exp_long, exh_long = check_register(long_ops)
    assert ok_long is True and exh_long is False and exp_long == 65

    # Budget exhaustion check on highly concurrent writes
    concurrent = [
        Operation(op_id=i, process=i, kind=OpKind.WRITE, key="c", value=i, call=0, ret=10)
        for i in range(8)
    ] + [Operation(op_id=99, process=9, kind=OpKind.READ, key="c", value=999, call=11, ret=12)]
    b_cpp = check_register(concurrent, budget=3, force_python=False)
    b_py = check_register(concurrent, budget=3, force_python=True)
    assert b_cpp == (True, 3, True)
    assert b_py == (True, 3, True)
