"""Tests for MemoryStorage, FileStorage, PosixDiskFS, SimulatedDiskFS, SegmentedWAL & SnapshotAssembler."""

from __future__ import annotations

from pathlib import Path

import pytest

from helios_consensus.core import (
    Entry,
    EntryKind,
    FileStorage,
    HardState,
    Membership,
    MemoryStorage,
    PosixDiskFS,
    Ready,
    SegmentedWAL,
    SimulatedDiskFS,
    Snapshot,
    SnapshotAssembler,
    SnapshotChunk,
    SnapshotIntegrityError,
    WalCorruptionError,
    split_snapshot,
)


def _boot() -> Snapshot:
    return Snapshot(index=0, term=0, membership=Membership(voters=frozenset({1, 2, 3})))


def test_memory_and_file_storage_crash_recovery(tmp_path: Path) -> None:
    boot = _boot()
    mem = MemoryStorage(boot)
    entries = [Entry(index=i, term=1, kind=EntryKind.NORMAL, data=f"k={i}") for i in range(1, 6)]
    mem.save(Ready(hard_state=HardState(term=1, vote=1, commit=4), entries=entries))
    mem.save(Ready(entries=[Entry(index=4, term=2, kind=EntryKind.NORMAL, data="overwritten")]))
    hs, sp, loaded = mem.load()
    assert hs.term == 1 and len(loaded) == 4 and loaded[-1].term == 2
    mem.save_snapshot(Snapshot(index=3, term=1, membership=boot.membership, data="s3"))
    assert [e.index for e in mem.load()[2]] == [4]

    # FileStorage with torn trailing bytes recovers valid prefix cleanly
    wal_dir = tmp_path / "fstore"
    fs = FileStorage(wal_dir, boot, fsync=True)
    fs.save(Ready(hard_state=HardState(term=2, vote=2, commit=3), entries=entries[:3]))
    fs.save_snapshot(Snapshot(index=2, term=1, membership=boot.membership, data="snap2"))
    fs.save(Ready(entries=entries[2:5], hard_state=HardState(term=2, vote=2, commit=5)))

    # Append 7 garbage torn bytes to wal.log and recover
    with (wal_dir / FileStorage.WAL).open("ab") as fh:
        fh.write(b"\xde\xad\xbe\xef\x01\x02\x03")
    fs_rec = FileStorage(wal_dir, boot, fsync=False)
    assert fs_rec.recovered_torn_bytes == 7
    hs2, sp2, ents2 = fs_rec.load()
    assert sp2.index == 2 and hs2.commit == 5 and [e.index for e in ents2] == [3, 4, 5]


def test_posix_and_simulated_disk_segmented_wal_rotation_gc_and_bitrot(tmp_path: Path) -> None:
    boot = _boot()
    with pytest.raises(ValueError, match="max_segment_bytes"):
        SegmentedWAL(SimulatedDiskFS(), boot, max_segment_bytes=64)

    # 1. PosixDiskFS end-to-end rotation, suffix overwrite, snapshot GC & recovery
    pfs = PosixDiskFS(tmp_path / "seg_posix")
    wal = SegmentedWAL(pfs, boot, max_segment_bytes=220, sync=True)
    for i in range(1, 16):
        wal.save(
            Ready(
                entries=[Entry(index=i, term=1, data=f"payload-{i:03d}")],
                hard_state=HardState(term=1, vote=1, commit=i),
            )
        )
    assert len(wal.segment_files()) >= 3
    # Overwrite suffix from index 12 onwards
    wal.save(
        Ready(
            entries=[
                Entry(index=12, term=2, data="new-12"),
                Entry(index=13, term=2, data="new-13"),
            ],
            hard_state=HardState(term=2, vote=2, commit=13),
        )
    )
    wal.save_snapshot(Snapshot(index=10, term=1, membership=boot.membership, data="snap10"))
    assert wal.segments_garbage_collected >= 1

    wal_reopened = SegmentedWAL(pfs, boot, max_segment_bytes=220, sync=False)
    hs, sp, ents = wal_reopened.load()
    assert hs.term == 2 and hs.commit == 13 and sp.index == 10
    assert [(e.index, e.term, e.data) for e in ents] == [
        (11, 1, "payload-011"),
        (12, 2, "new-12"),
        (13, 2, "new-13"),
    ]

    # 2. SimulatedDiskFS bitrot & torn header/payload detection
    sim_fs = SimulatedDiskFS(seed=9)
    with pytest.raises(FileNotFoundError):
        sim_fs.read("missing")
    with pytest.raises(FileNotFoundError):
        sim_fs.truncate("missing", 0, sync=True)
    with pytest.raises(FileNotFoundError):
        sim_fs.inject_bitrot("missing", 0)

    swal = SegmentedWAL(sim_fs, boot, max_segment_bytes=180, sync=True)
    for i in range(1, 12):
        swal.save(Ready(entries=[Entry(index=i, term=1, data=f"row-{i:04d}")]))
    sim_fs.fsync_all()
    segs = swal.segment_files()
    assert len(segs) >= 2

    # Silent bitrot inside a sealed non-tail segment MUST raise WalCorruptionError
    sim_fs.inject_bitrot(segs[0], byte_offset=12, mask=0x04)
    with pytest.raises(WalCorruptionError, match="sealed WAL segment"):
        SegmentedWAL(sim_fs, boot, max_segment_bytes=180)

    # Corrupt snapshot file MUST raise WalCorruptionError
    sim_fs2 = SimulatedDiskFS(seed=10)
    swal2 = SegmentedWAL(sim_fs2, boot, max_segment_bytes=512, sync=True)
    swal2.save(Ready(entries=[Entry(index=1, term=1, data="a")], hard_state=HardState(1, 1, 1)))
    swal2.save_snapshot(Snapshot(index=1, term=1, membership=boot.membership, data="ok"))
    sim_fs2.inject_bitrot(SegmentedWAL.SNAP_FILE, byte_offset=14, mask=0x08)
    with pytest.raises(WalCorruptionError, match="corrupt snapshot"):
        SegmentedWAL(sim_fs2, boot, max_segment_bytes=512)

    # Torn un-fsynced tail on SimulatedDiskFS & PosixDiskFS is truncated cleanly on recovery
    sim_fs3 = SimulatedDiskFS(seed=3)
    swal3 = SegmentedWAL(sim_fs3, boot, max_segment_bytes=512, sync=True)
    swal3.save(
        Ready(entries=[Entry(index=1, term=1, data="durable")], hard_state=HardState(1, 1, 1))
    )
    tail_seg = swal3.segment_files()[-1]
    sim_fs3.append(tail_seg, b"\x05\x00\x00\x00\xaa\xbb\xcc", sync=False)
    sim_fs3.power_cut(tear_unsynced_tail=True)
    swal3_rec = SegmentedWAL(sim_fs3, boot, max_segment_bytes=512, sync=True)
    assert [e.data for e in swal3_rec.load()[2]] == ["durable"]

    pfs_tail = wal.segment_files()[-1]
    pfs.append(pfs_tail, b"\x09\x00\x00\x00\x11\x22", sync=True)
    wal_trunc = SegmentedWAL(pfs, boot, max_segment_bytes=220, sync=True)
    assert wal_trunc.truncated_torn_bytes == 6


def test_snapshot_chunker_and_assembler_edge_cases() -> None:
    snap = Snapshot(
        index=25,
        term=4,
        membership=Membership(voters=frozenset({1, 2, 3}), voters_outgoing=frozenset({2, 3, 4})),
        data="state=" + ("Z" * 3000),
    )
    with pytest.raises(ValueError, match="chunk_bytes"):
        split_snapshot(snap, term=4, chunk_bytes=0)

    chunks = split_snapshot(snap, term=4, chunk_bytes=512)
    asm = SnapshotAssembler()

    # Out-of-bounds chunk rejected
    bad_bounds = SnapshotChunk(
        4, 25, 4, chunk_id=99, total_chunks=len(chunks), crc32c=0, merkle_root_hex="00", payload=b""
    )
    ack, out = asm.ingest(bad_bounds)
    assert not ack.accepted and ack.reason == "invalid_chunk_bounds" and out is None

    # Feed first two chunks, then test stale snapshot rejection
    asm.ingest(chunks[0])
    asm.ingest(chunks[1])
    stale = SnapshotChunk(
        3,
        10,
        2,
        chunk_id=0,
        total_chunks=1,
        crc32c=chunks[0].crc32c,
        merkle_root_hex="aa",
        payload=chunks[0].payload,
    )
    ack_stale, _ = asm.ingest(stale)
    assert not ack_stale.accepted and ack_stale.reason == "stale_snapshot_index"

    # Duplicate delivery + out-of-order completion succeeds
    asm.ingest(chunks[0])
    completed_snap = None
    for ch in reversed(chunks[1:]):
        ack, maybe = asm.ingest(ch)
        if maybe is not None:
            completed_snap = maybe
    assert completed_snap == snap

    # Tampered Merkle root with valid per-chunk CRC32C raises SnapshotIntegrityError!
    asm_tamper = SnapshotAssembler()
    for ch in chunks[:-1]:
        tampered = SnapshotChunk(
            ch.term,
            ch.snapshot_index,
            ch.snapshot_term,
            ch.chunk_id,
            ch.total_chunks,
            ch.crc32c,
            "f" * 64,
            ch.payload,
        )
        asm_tamper.ingest(tampered)
    last = chunks[-1]
    with pytest.raises(SnapshotIntegrityError, match="Merkle root mismatch"):
        asm_tamper.ingest(
            SnapshotChunk(
                last.term,
                last.snapshot_index,
                last.snapshot_term,
                last.chunk_id,
                last.total_chunks,
                last.crc32c,
                "f" * 64,
                last.payload,
            )
        )
