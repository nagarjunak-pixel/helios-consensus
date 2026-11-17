"""Tests for CRC32C wire codec, asyncio TCP Raft cluster/client, and Jump-Hash Multi-Raft."""

from __future__ import annotations

import asyncio
import struct
from pathlib import Path

import pytest

from helios_consensus.core.messages import (
    ConfChange,
    ConfChangeType,
    Entry,
    EntryKind,
    Membership,
    Message,
    MsgType,
    Snapshot,
)
from helios_consensus.core.node import RaftConfig
from helios_consensus.core.segmented_wal import PosixDiskFS, SegmentedWAL
from helios_consensus.core.snapshot_stream import split_snapshot
from helios_consensus.multiraft import MultiRaftCluster
from helios_consensus.net import ClusterUnavailableError, RaftKVClient, RaftTCPServer
from helios_consensus.net.codec import (
    WireFrameError,
    chunk_from_dict,
    chunk_to_dict,
    message_from_dict,
    message_to_dict,
    pack_envelope,
    read_envelope,
    unpack_payload,
)


async def test_wire_codec_roundtrip_and_corruption_errors() -> None:
    snap = Snapshot(
        index=12,
        term=3,
        membership=Membership(voters=frozenset({1, 2, 3}), learners=frozenset({4})),
        data='{"k": 99}',
    )
    msg = Message(
        type=MsgType.APPEND,
        src=1,
        dst=2,
        term=3,
        log_term=2,
        index=12,
        entries=(Entry(index=13, term=3, kind=EntryKind.NORMAL, data="put:x=1"),),
        commit=12,
        snapshot=snap,
        context=77,
    )
    assert message_from_dict(message_to_dict(msg)) == msg

    chunks = split_snapshot(snap, term=3, chunk_bytes=16)
    assert len(chunks) >= 1
    for ch in chunks:
        assert chunk_from_dict(chunk_to_dict(ch)) == ch

    frame = pack_envelope({"hello": "world", "n": 42})
    reader = asyncio.StreamReader()
    reader.feed_data(frame)
    reader.feed_eof()
    assert await read_envelope(reader) == {"hello": "world", "n": 42}
    assert await read_envelope(reader) is None  # clean EOF

    with pytest.raises(WireFrameError, match="CRC32C"):
        unpack_payload(b'{"hello":"tampered"}', 0x12345678)

    # Truncated header
    r_hdr = asyncio.StreamReader()
    r_hdr.feed_data(b"\x01\x02\x03")
    r_hdr.feed_eof()
    with pytest.raises(WireFrameError, match="header"):
        await read_envelope(r_hdr)

    # Oversized frame
    r_big = asyncio.StreamReader()
    r_big.feed_data(struct.pack("<II", 32 * 1024 * 1024, 0))
    r_big.feed_eof()
    with pytest.raises(WireFrameError, match="oversized"):
        await read_envelope(r_big)

    # Truncated payload
    r_pay = asyncio.StreamReader()
    r_pay.feed_data(frame[:-4])
    r_pay.feed_eof()
    with pytest.raises(WireFrameError, match="payload"):
        await read_envelope(r_pay)


async def test_asyncio_tcp_cluster_failover_reconfig_and_chunked_snapshot_sync(
    tmp_path: Path,
) -> None:
    snap0 = Snapshot(index=0, term=0, membership=Membership(voters=frozenset({1, 2, 3})))
    cfg = RaftConfig(election_tick=10, heartbeat_tick=2)

    def make_wal(nid: int, initial: Snapshot | None = None) -> SegmentedWAL:
        boot = initial or Snapshot(index=0, term=0, membership=Membership(voters=frozenset()))
        return SegmentedWAL(
            PosixDiskFS(tmp_path / f"node_{nid}"),
            boot,
            max_segment_bytes=1024,
        )

    servers: dict[int, RaftTCPServer] = {}
    endpoints: dict[int, tuple[str, int]] = {}
    for nid in (1, 2, 3):
        srv = RaftTCPServer(
            nid,
            {},
            make_wal(nid, snap0),
            config=cfg,
            tick_interval=0.01,
            snapshot_threshold=8,
            snapshot_chunk_bytes=96,
            seed=nid * 13,
        )
        endpoints[nid] = await srv.start()
        servers[nid] = srv

    for srv in servers.values():
        srv.update_peers(endpoints)

    try:
        client = RaftKVClient(endpoints, client_id=101, rpc_timeout=0.7, max_attempts=30)
        assert await client.put("alpha", 10) is True
        assert await client.cas("alpha", 10, 25) is True
        assert await client.cas("alpha", 999, 50) is False
        assert await client.get("alpha") == 25
        assert await client.get("missing") is None

        st1 = await client.status(1)
        assert st1 is not None and "role" in st1

        # Add node 4 dynamically as learner and promote to voter
        srv4 = RaftTCPServer(
            4,
            endpoints,
            make_wal(4, Snapshot(index=0, term=0, membership=Membership(voters=frozenset()))),
            config=cfg,
            tick_interval=0.01,
            snapshot_threshold=8,
            snapshot_chunk_bytes=96,
            seed=99,
        )
        addr4 = await srv4.start()
        endpoints[4] = addr4
        servers[4] = srv4
        for srv in servers.values():
            srv.update_peers(endpoints)
        client.update_endpoints(endpoints)

        await client.propose_conf_change(ConfChange(ConfChangeType.ADD_LEARNER, 4))
        await asyncio.sleep(0.18)
        await client.propose_conf_change(ConfChange(ConfChangeType.PROMOTE, 4))
        await asyncio.sleep(0.15)

        # Find current leader, stop it, write > snapshot_threshold keys so remaining quorum compacts
        leader_id = client.leader_hint or 1
        stopped_port = endpoints[leader_id][1]
        await servers[leader_id].stop()

        for idx in range(14):
            assert await client.put(f"k_{idx}", idx * 3) is True
        assert await client.get("k_13") == 39

        # Restart stopped server on the exact same port from SegmentedWAL; must catch up via SnapshotChunk!
        restarted = RaftTCPServer(
            leader_id,
            endpoints,
            make_wal(leader_id),
            port=stopped_port,
            config=cfg,
            tick_interval=0.01,
            snapshot_threshold=8,
            snapshot_chunk_bytes=96,
            seed=leader_id * 17,
        )
        await restarted.start()
        servers[leader_id] = restarted

        caught_up = False
        for _ in range(40):
            await asyncio.sleep(0.05)
            if restarted.sm.read("k_13") == 39:
                caught_up = True
                break
        assert caught_up is True
    finally:
        for srv in list(servers.values()):
            await srv.stop()

    # All servers stopped -> client exhausts retry budget cleanly
    dead_client = RaftKVClient(endpoints, rpc_timeout=0.05, max_attempts=3)
    with pytest.raises(ClusterUnavailableError):
        await dead_client.get("alpha")
    with pytest.raises(ClusterUnavailableError, match="no cluster endpoints"):
        await RaftKVClient({}).get("alpha")


def test_multiraft_sharding_range_split_session_dedup_and_rebalancing() -> None:
    with pytest.raises(ValueError, match="replicas_per_range"):
        MultiRaftCluster(num_stores=3, replicas_per_range=5)

    mr = MultiRaftCluster(
        num_stores=5,
        initial_boundaries=["g", "n", "t"],
        replicas_per_range=3,
        seed=7,
    )
    mr.wait_all_leaders()
    assert len(mr.ranges) == 4

    keys = {
        "apple": 11,
        "charlie": 22,
        "echo": 33,
        "hotel": 44,
        "mike": 55,
        "oscar": 66,
        "victor": 77,
    }
    for seq, (k, v) in enumerate(keys.items(), start=1):
        mr.put(k, v, client_id=500, seq=seq)
    mr.step_ticks(3)

    for k, v in keys.items():
        assert mr.get_linearizable(k) == v

    first_range = mr.locate_range("apple")
    with pytest.raises(ValueError, match="split_key"):
        mr.split_range(first_range.range_id, "zulu")
    with pytest.raises(ValueError, match="split_key"):
        mr.split_range(first_range.range_id, first_range.start_key)

    left, right = mr.split_range(first_range.range_id, "d")
    assert (left.start_key, left.end_key) == ("", "d")
    assert (right.start_key, right.end_key) == ("d", "g")
    assert left.epoch == first_range.epoch + 1
    assert len(mr.ranges) == 5

    # Keys on both sides of the split remain intact & linearizable
    assert mr.get_linearizable("apple") == 11
    assert mr.get_linearizable("charlie") == 22
    assert mr.get_linearizable("echo") == 33

    # Retrying seq=3 ("echo") with a DIFFERENT value after split must be deduplicated by cloned session table!
    mr.put("echo", 99999, client_id=500, seq=3)
    mr.step_ticks(3)
    assert mr.get_linearizable("echo") == 33

    # New sequence number succeeds on post-split right range
    mr.put("echo", 330, client_id=500, seq=99)
    mr.step_ticks(3)
    assert mr.get_linearizable("echo") == 330

    # Rebalance range leaders across the 5 physical stores via TimeoutNow
    balanced = mr.rebalance_leaders()
    assert sum(balanced.values()) == len(mr.ranges)
    assert max(balanced.values()) - min(balanced.values()) <= 2
