"""CRC32C-authenticated binary framing and JSON wire codec for TCP Raft RPCs."""

from __future__ import annotations

import asyncio
import json
import struct
from typing import Any

from helios_consensus.core.messages import (
    Entry,
    EntryKind,
    Message,
    MsgType,
)
from helios_consensus.core.snapshot_stream import SnapshotChunk
from helios_consensus.core.storage import snapshot_from_json, snapshot_to_json
from helios_consensus.kernels.native_ring import get_helios_bridge

_FRAME_HDR = struct.Struct("<II")  # length, crc32c
_MAX_WIRE_BYTES = 16 * 1024 * 1024


class WireFrameError(ValueError):
    """Raised on corrupt CRC32C or oversized TCP wire frame."""


def pack_envelope(obj: dict[str, Any]) -> bytes:
    raw = json.dumps(obj, separators=(",", ":"), sort_keys=True).encode("utf-8")
    crc = get_helios_bridge().crc32c(raw)
    return _FRAME_HDR.pack(len(raw), crc) + raw


def unpack_payload(raw: bytes, expected_crc: int) -> dict[str, Any]:
    actual = get_helios_bridge().crc32c(raw)
    if actual != expected_crc:
        raise WireFrameError(f"TCP frame CRC32C mismatch: got {actual:#x}, want {expected_crc:#x}")
    res: dict[str, Any] = json.loads(raw.decode("utf-8"))
    return res


async def read_envelope(reader: asyncio.StreamReader) -> dict[str, Any] | None:
    try:
        hdr = await reader.readexactly(_FRAME_HDR.size)
    except asyncio.IncompleteReadError as exc:
        if not exc.partial:
            return None
        raise WireFrameError("truncated TCP frame header") from exc
    length, crc = _FRAME_HDR.unpack(hdr)
    if length > _MAX_WIRE_BYTES:
        raise WireFrameError(f"oversized TCP frame ({length} bytes)")
    try:
        body = await reader.readexactly(length)
    except asyncio.IncompleteReadError as exc:
        raise WireFrameError("truncated TCP frame payload") from exc
    return unpack_payload(body, crc)


def message_to_dict(m: Message) -> dict[str, Any]:
    return {
        "type": m.type.value,
        "src": m.src,
        "dst": m.dst,
        "term": m.term,
        "log_term": m.log_term,
        "index": m.index,
        "entries": [{"i": e.index, "t": e.term, "k": e.kind.value, "d": e.data} for e in m.entries],
        "commit": m.commit,
        "reject": m.reject,
        "reject_hint": m.reject_hint,
        "snapshot": snapshot_to_json(m.snapshot) if m.snapshot is not None else None,
        "context": m.context,
    }


def message_from_dict(d: dict[str, Any]) -> Message:
    snap_raw = d.get("snapshot")
    return Message(
        type=MsgType(d["type"]),
        src=int(d["src"]),
        dst=int(d["dst"]),
        term=int(d["term"]),
        log_term=int(d.get("log_term", 0)),
        index=int(d.get("index", 0)),
        entries=tuple(
            Entry(index=int(e["i"]), term=int(e["t"]), kind=EntryKind(e["k"]), data=str(e["d"]))
            for e in d.get("entries", [])
        ),
        commit=int(d.get("commit", 0)),
        reject=bool(d.get("reject", False)),
        reject_hint=int(d.get("reject_hint", 0)),
        snapshot=snapshot_from_json(snap_raw) if snap_raw is not None else None,
        context=int(d.get("context", 0)),
    )


def chunk_to_dict(c: SnapshotChunk) -> dict[str, Any]:
    return {
        "term": c.term,
        "snapshot_index": c.snapshot_index,
        "snapshot_term": c.snapshot_term,
        "chunk_id": c.chunk_id,
        "total_chunks": c.total_chunks,
        "crc32c": c.crc32c,
        "merkle_root_hex": c.merkle_root_hex,
        "payload_hex": c.payload.hex(),
    }


def chunk_from_dict(d: dict[str, Any]) -> SnapshotChunk:
    return SnapshotChunk(
        term=int(d["term"]),
        snapshot_index=int(d["snapshot_index"]),
        snapshot_term=int(d["snapshot_term"]),
        chunk_id=int(d["chunk_id"]),
        total_chunks=int(d["total_chunks"]),
        crc32c=int(d["crc32c"]),
        merkle_root_hex=str(d["merkle_root_hex"]),
        payload=bytes.fromhex(str(d["payload_hex"])),
    )
