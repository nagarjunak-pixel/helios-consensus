"""Focused unit & parameterized tests for Membership, InvariantObserver, SimNetwork & KVStateMachine."""

from __future__ import annotations

import random

import pytest

from helios_consensus.check.invariants import InvariantObserver
from helios_consensus.core.messages import (
    ConfChange,
    ConfChangeType,
    Entry,
    EntryKind,
    Membership,
    Message,
    MsgType,
    Ready,
    Snapshot,
)
from helios_consensus.core.node import RaftConfig, RaftNode, StateRole
from helios_consensus.kv.state_machine import KVCommand, KVOp, KVStateMachine
from helios_consensus.sim.network import SimNetwork


@pytest.mark.parametrize(
    "cc",
    [
        ConfChange(ConfChangeType.ADD_LEARNER, node_id=4),
        ConfChange(ConfChangeType.PROMOTE, node_id=4),
        ConfChange(ConfChangeType.REMOVE, node_id=2),
        ConfChange(ConfChangeType.ENTER_JOINT, voters=frozenset({4, 5, 6})),
        ConfChange(ConfChangeType.LEAVE_JOINT),
    ],
)
def test_conf_change_encode_decode_roundtrip(cc: ConfChange) -> None:
    assert ConfChange.decode(cc.encode()) == cc


@pytest.mark.parametrize(
    ("acked", "expected"),
    [
        (frozenset({1, 2, 4, 5}), True),
        (frozenset({2, 3, 5, 6}), True),
        (frozenset({1, 2, 3}), False),  # missing C_new majority
        (frozenset({4, 5, 6}), False),  # missing C_old majority
        (frozenset({1, 4}), False),
    ],
)
def test_joint_consensus_dual_quorum_truth_table(acked: frozenset[int], expected: bool) -> None:
    m_old = Membership(voters=frozenset({1, 2, 3}))
    m_joint = m_old.apply(ConfChange(ConfChangeType.ENTER_JOINT, voters=frozenset({4, 5, 6})))
    assert m_joint.is_joint is True
    assert m_joint.has_quorum(acked) is expected


@pytest.mark.parametrize(
    ("votes", "expected_won", "expected_lost"),
    [
        ({1: True, 2: True, 4: True, 5: True}, True, False),
        ({1: True, 2: True, 4: False, 5: False}, False, True),
        ({1: False, 2: False, 4: True, 5: True}, False, True),
        ({1: True, 4: True}, False, False),
    ],
)
def test_joint_consensus_vote_tally(
    votes: dict[int, bool], expected_won: bool, expected_lost: bool
) -> None:
    m_joint = Membership(voters=frozenset({4, 5, 6}), voters_outgoing=frozenset({1, 2, 3}))
    assert m_joint.tally_votes(votes) == (expected_won, expected_lost)


def test_membership_invalid_transitions_and_ready_empty() -> None:
    with pytest.raises(ValueError, match="both voter and learner"):
        Membership(voters=frozenset({1, 2}), learners=frozenset({2}))

    m = Membership(voters=frozenset({1}))
    with pytest.raises(ValueError, match="already a voter"):
        m.apply(ConfChange(ConfChangeType.ADD_LEARNER, 1))
    with pytest.raises(ValueError, match="must be a learner"):
        m.apply(ConfChange(ConfChangeType.PROMOTE, 9))
    with pytest.raises(ValueError, match="not a member"):
        m.apply(ConfChange(ConfChangeType.REMOVE, 9))
    with pytest.raises(ValueError, match="last voter"):
        m.apply(ConfChange(ConfChangeType.REMOVE, 1))
    with pytest.raises(ValueError, match="not in joint"):
        m.apply(ConfChange(ConfChangeType.LEAVE_JOINT))
    with pytest.raises(ValueError, match="cannot be empty"):
        m.apply(ConfChange(ConfChangeType.ENTER_JOINT, voters=frozenset()))

    joint = m.apply(ConfChange(ConfChangeType.ENTER_JOINT, voters=frozenset({2, 3})))
    with pytest.raises(ValueError, match="already in joint"):
        joint.apply(ConfChange(ConfChangeType.ENTER_JOINT, voters=frozenset({4})))
    with pytest.raises(ValueError, match="single-server change"):
        joint.apply(ConfChange(ConfChangeType.ADD_LEARNER, 5))

    left = joint.apply(ConfChange(ConfChangeType.LEAVE_JOINT))
    assert not left.is_joint and left.voters == frozenset({2, 3})
    assert Ready().is_empty() is True


def test_invariant_observer_detects_all_four_raft_violations() -> None:
    boot = Snapshot(index=0, term=0, membership=Membership(voters=frozenset({1, 2, 3})))
    cfg = RaftConfig(election_tick=10, heartbeat_tick=2)
    n1 = RaftNode(1, cfg, seed=1, snapshot=boot)
    n2 = RaftNode(2, cfg, seed=2, snapshot=boot)

    obs = InvariantObserver()
    # 1. StateMachineSafety: out-of-order gap & conflicting entry at same index
    e1 = Entry(index=1, term=1, data="x=1")
    e3_gap = Entry(index=3, term=1, data="x=3")
    obs.on_apply(1, 0, e1, tick=1)
    obs.on_apply(1, 0, e3_gap, tick=2)
    obs.on_apply(2, 0, Entry(index=1, term=2, data="DIFF"), tick=3)
    assert sum(1 for v in obs.violations if v.invariant == "StateMachineSafety") == 2

    # 2. ElectionSafety & LeaderCompleteness
    n1.role = StateRole.LEADER
    n1.term = 2
    n1.log.append([e1])
    n2.role = StateRole.LEADER
    n2.term = 2  # second leader in term 2 with empty log (missing applied e1!)
    obs.check_roles([n1, n2], tick=4)
    invs = {v.invariant for v in obs.violations}
    assert "ElectionSafety" in invs and "LeaderCompleteness" in invs

    # 3. LogMatching: two logs agree on term at index 2 but differ at index 1!
    n3 = RaftNode(3, cfg, seed=3, snapshot=boot)
    n4 = RaftNode(4, cfg, seed=4, snapshot=boot)
    n3.log.append([Entry(index=1, term=1, data="a"), Entry(index=2, term=2, data="same")])
    n4.log.append([Entry(index=1, term=9, data="b"), Entry(index=2, term=2, data="same")])
    obs.check_log_matching([n3, n4], tick=5)
    assert any(v.invariant == "LogMatching" for v in obs.violations)


@pytest.mark.parametrize(
    ("op", "key", "val", "expected_prev", "want_ok", "want_state"),
    [
        (KVOp.PUT, "k", 10, None, True, 10),
        (KVOp.CAS, "absent", 5, None, True, 5),
        (KVOp.CAS, "absent", 5, 99, False, None),
    ],
)
def test_kv_state_machine_ops_dedup_and_snapshot_restore(
    op: KVOp,
    key: str,
    val: int,
    expected_prev: int | None,
    want_ok: bool,
    want_state: int | None,
) -> None:
    sm = KVStateMachine()
    assert sm.apply(Entry(index=1, term=1, kind=EntryKind.NOOP)) is None
    cmd = KVCommand(op=op, key=key, value=val, expected=expected_prev, client=7, seq=1)
    res = sm.apply(Entry(index=2, term=1, kind=EntryKind.NORMAL, data=cmd.encode()))
    assert res is not None and res.result is want_ok and not res.duplicate
    assert sm.read(key) == want_state

    # Duplicate client sequence number returns cached result without mutating state
    dup_cmd = KVCommand(op=KVOp.PUT, key=key, value=9999, client=7, seq=1)
    dup_res = sm.apply(Entry(index=3, term=1, kind=EntryKind.NORMAL, data=dup_cmd.encode()))
    assert dup_res is not None and dup_res.duplicate is True and dup_res.result is want_ok
    assert sm.read(key) == want_state

    with pytest.raises(ValueError, match="already applied"):
        sm.apply(Entry(index=3, term=1, kind=EntryKind.NOOP))

    restored = KVStateMachine.restore(sm.snapshot())
    assert restored.digest() == sm.digest()
    assert restored.applied_index == sm.applied_index


def test_sim_network_bridge_drop_duplication_and_heal() -> None:
    net = SimNetwork(rng=random.Random(1), min_delay=1, max_delay=2, drop_rate=0.0, dup_rate=1.0)
    net.bridge({1, 2}, {4, 5}, bridge_node=3)

    # 1 -> 4 is blocked directly, but 1 -> 3 and 3 -> 4 pass through bridge node 3!
    net.send(Message(type=MsgType.HEARTBEAT, src=1, dst=4, term=1), now=0)
    assert net.stats.dropped_partition == 1
    net.send(Message(type=MsgType.HEARTBEAT, src=1, dst=3, term=1), now=0)
    assert net.in_flight() == 2  # dup_rate=1.0 duplicated the packet!
    assert net.stats.duplicated == 1

    delivered = net.due(now=10)
    assert len(delivered) == 2

    # Partition while message is already in flight drops it on delivery
    net.defer(Message(type=MsgType.HEARTBEAT, src=3, dst=4, term=1), at=15)
    net.isolate({3}, {4})
    assert net.due(now=20) == []
    assert net.stats.dropped_partition == 2

    net.heal()
    net.drop_rate = 1.0
    net.send(Message(type=MsgType.HEARTBEAT, src=3, dst=4, term=1), now=21)
    assert net.stats.dropped_loss == 1
