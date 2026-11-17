"""Chunked snapshot streaming with per-chunk CRC32C and RFC-6962 Merkle root verification.

Production Raft engines avoid sending multi-megabyte snapshots in one monolithic RPC
frame because a single bit error or dropped packet forces retransmitting the entire image,
and giant frames block heartbeat messages on shared sockets. ``SnapshotChunker`` splits a
``Snapshot`` into fixed-size chunks protected individually by Castagnoli CRC32C and
collectively by an RFC-6962 SHA-256 Merkle tree root; ``SnapshotAssembler`` reassembles
chunks idempotently, tolerates duplicates and out-of-order arrival, and yields a verified
``Snapshot`` only when both every chunk CRC and the Merkle root match.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

from helios_consensus.core.messages import Snapshot
from helios_consensus.core.storage import snapshot_from_json, snapshot_to_json
from helios_consensus.kernels.native_ring import get_helios_bridge


class SnapshotIntegrityError(ValueError):
    """Raised when a reassembled snapshot fails Merkle root or schema validation."""


@dataclass(frozen=True, slots=True)
class SnapshotChunk:
    term: int
    snapshot_index: int
    snapshot_term: int
    chunk_id: int
    total_chunks: int
    crc32c: int
    merkle_root_hex: str
    payload: bytes


@dataclass(frozen=True, slots=True)
class ChunkAck:
    accepted: bool
    completed: bool
    received_chunks: int
    total_chunks: int
    next_missing_chunk: int
    reason: str = "ok"


def split_snapshot(
    snapshot: Snapshot, *, term: int, chunk_bytes: int = 4096
) -> list[SnapshotChunk]:
    """Serialize ``snapshot`` and split into CRC32C + Merkle-authenticated chunks."""
    if chunk_bytes <= 0:
        raise ValueError("chunk_bytes must be > 0")
    raw = json.dumps(snapshot_to_json(snapshot), separators=(",", ":"), sort_keys=True).encode()
    bridge = get_helios_bridge()
    parts = [raw[i : i + chunk_bytes] for i in range(0, len(raw), chunk_bytes)] or [b""]
    root_hex = bridge.merkle_root_rfc6962(parts).hex()
    total = len(parts)
    return [
        SnapshotChunk(
            term=term,
            snapshot_index=snapshot.index,
            snapshot_term=snapshot.term,
            chunk_id=idx,
            total_chunks=total,
            crc32c=bridge.crc32c(part),
            merkle_root_hex=root_hex,
            payload=part,
        )
        for idx, part in enumerate(parts)
    ]


@dataclass(slots=True)
class SnapshotAssembler:
    """Stateful receiver reassembling chunked snapshots for a follower."""

    snapshot_index: int = 0
    snapshot_term: int = 0
    total_chunks: int = 0
    merkle_root_hex: str = ""
    chunks: dict[int, bytes] = field(default_factory=dict)
    completed_snapshot: Snapshot | None = None

    def reset(self) -> None:
        self.snapshot_index = 0
        self.snapshot_term = 0
        self.total_chunks = 0
        self.merkle_root_hex = ""
        self.chunks.clear()
        self.completed_snapshot = None

    def _next_missing(self) -> int:
        for i in range(self.total_chunks):
            if i not in self.chunks:
                return i
        return self.total_chunks

    def ingest(self, chunk: SnapshotChunk) -> tuple[ChunkAck, Snapshot | None]:
        """Process one incoming chunk; returns ``(ack, completed_snapshot_or_none)``."""
        if chunk.total_chunks <= 0 or not (0 <= chunk.chunk_id < chunk.total_chunks):
            return (
                ChunkAck(
                    accepted=False,
                    completed=False,
                    received_chunks=len(self.chunks),
                    total_chunks=max(0, self.total_chunks),
                    next_missing_chunk=self._next_missing(),
                    reason="invalid_chunk_bounds",
                ),
                None,
            )

        if chunk.snapshot_index < self.snapshot_index:
            return (
                ChunkAck(
                    accepted=False,
                    completed=self.completed_snapshot is not None,
                    received_chunks=len(self.chunks),
                    total_chunks=self.total_chunks,
                    next_missing_chunk=self._next_missing(),
                    reason="stale_snapshot_index",
                ),
                None,
            )

        bridge = get_helios_bridge()
        if bridge.crc32c(chunk.payload) != chunk.crc32c:
            return (
                ChunkAck(
                    accepted=False,
                    completed=False,
                    received_chunks=len(self.chunks),
                    total_chunks=self.total_chunks or chunk.total_chunks,
                    next_missing_chunk=chunk.chunk_id,
                    reason="crc32c_mismatch",
                ),
                None,
            )

        # Supersede any older or partially transferred snapshot if metadata advances.
        if (
            chunk.snapshot_index > self.snapshot_index
            or self.total_chunks == 0
            or (
                chunk.snapshot_index == self.snapshot_index
                and chunk.merkle_root_hex != self.merkle_root_hex
            )
        ):
            self.snapshot_index = chunk.snapshot_index
            self.snapshot_term = chunk.snapshot_term
            self.total_chunks = chunk.total_chunks
            self.merkle_root_hex = chunk.merkle_root_hex
            self.chunks.clear()
            self.completed_snapshot = None

        self.chunks[chunk.chunk_id] = chunk.payload

        if len(self.chunks) < self.total_chunks:
            return (
                ChunkAck(
                    accepted=True,
                    completed=False,
                    received_chunks=len(self.chunks),
                    total_chunks=self.total_chunks,
                    next_missing_chunk=self._next_missing(),
                ),
                None,
            )

        ordered = [self.chunks[i] for i in range(self.total_chunks)]
        actual_root = bridge.merkle_root_rfc6962(ordered).hex()
        if actual_root != self.merkle_root_hex:
            self.reset()
            raise SnapshotIntegrityError(
                f"snapshot Merkle root mismatch: expected {chunk.merkle_root_hex}, got {actual_root}"
            )

        snap = snapshot_from_json(json.loads(b"".join(ordered).decode()))
        if snap.index != self.snapshot_index or snap.term != self.snapshot_term:
            self.reset()
            raise SnapshotIntegrityError("reassembled snapshot header contradicts chunk metadata")
        self.completed_snapshot = snap
        return (
            ChunkAck(
                accepted=True,
                completed=True,
                received_chunks=self.total_chunks,
                total_chunks=self.total_chunks,
                next_missing_chunk=self.total_chunks,
            ),
            snap,
        )
