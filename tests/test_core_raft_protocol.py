"""Deterministic protocol tests for RaftNode: Figure 8, PreVote, CheckQuorum, ReadIndex, Joint Consensus & Snapshots."""

from __future__ import annotations

import pytest

from helios_consensus.core import (
    ConfChange,
    ConfChangeType,
    Membership,
    Message,
    MsgType,
    NotLeaderError,
    ProposalDroppedError,
    RaftConfig,
    RaftNode,
    Snapshot,
    StateRole,
)


def _make_cluster(ids: list[int], cfg: RaftConfig | None = None) -> dict[int, RaftNode]:
    rcfg = cfg or RaftConfig(election_tick=10, heartbeat_tick=2)
    boot = Snapshot(index=0, term=0, membership=Membership(voters=frozenset(ids)))
    return {i: RaftNode(i, rcfg, seed=i, snapshot=boot) for i in ids}


def _deliver_all(
    nodes: dict[int, RaftNode],
    *,
    blocked: set[tuple[int, int]] | None = None,
    drop_types: set[MsgType] | None = None,
) -> list[Message]:
    sent: list[Message] = []
    cut = blocked or set()
    drops = drop_types or set()
    for _ in range(80):
        moved = False
        for n in list(nodes.values()):
            if n.has_ready():
                rd = n.ready()
                n.advance(rd)
                for m in rd.messages:
                    sent.append(m)
                    if (m.src, m.dst) in cut or m.type in drops:
                        continue
                    if m.dst in nodes:
                        nodes[m.dst].step(m)
                        moved = True
        if not moved:
            break
    return sent


def test_config_and_membership_validation_guards() -> None:
    with pytest.raises(ValueError, match="election_tick"):
        RaftConfig(election_tick=3, heartbeat_tick=3)
    with pytest.raises(ValueError, match="unknown bug"):
        RaftConfig(bugs=frozenset({"made_up_bug"}))
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

    jm = m.apply(ConfChange(ConfChangeType.ENTER_JOINT, voters=frozenset({2, 3})))
    assert jm.is_joint
    with pytest.raises(ValueError, match="already in joint"):
        jm.apply(ConfChange(ConfChangeType.ENTER_JOINT, voters=frozenset({4})))
    with pytest.raises(ValueError, match="while in joint"):
        jm.apply(ConfChange(ConfChangeType.ADD_LEARNER, 5))


def _run_figure_8_scenario(*, buggy_commit_prior_term: bool) -> tuple[bool, list[str]]:
    """Construct Ongaro & Ousterhout (2014) Figure 8 step-by-step on a 5-node cluster.

    Returns ``(committed_entry_overwritten, [node_1_log_at_idx2_after_term4, ...])``.
    """
    bugs = frozenset({"commit_prior_term"}) if buggy_commit_prior_term else frozenset()
    cfg = RaftConfig(
        election_tick=10,
        heartbeat_tick=2,
        max_entries_per_msg=1,
        pre_vote=False,
        check_quorum=False,
        bugs=bugs,
    )
    nodes = _make_cluster([1, 2, 3, 4, 5], cfg)

    # (a) Term 1: S1 wins election across all 5 nodes, commits index 1 (if normal), then appends
    # 'x=1_term1' and replicates it ONLY to S2 (blocked from S3, S4, S5).
    nodes[1].campaign()
    _deliver_all(nodes)
    assert nodes[1].role is StateRole.LEADER and nodes[1].term == 1
    idx_fig8 = nodes[1].propose("x=1_term1")
    for _ in range(2):
        nodes[1].tick()
    cut_s1_minority = {(i, j) for i in (1, 2) for j in (3, 4, 5)} | {
        (j, i) for i in (1, 2) for j in (3, 4, 5)
    }
    _deliver_all(nodes, blocked=cut_s1_minority)
    assert nodes[1].log.term(idx_fig8) == 1 and nodes[2].log.term(idx_fig8) == 1
    assert nodes[3].log.last_index < idx_fig8

    # (b) Term 2: S5 wins election with votes from {S3, S4, S5} and appends 'x=2_term2' locally
    # without replicating any term-2 entry to {S3, S4} before being isolated.
    nodes[5].campaign()
    _deliver_all(nodes, blocked=cut_s1_minority, drop_types={MsgType.APPEND})
    assert nodes[5].role is StateRole.LEADER and nodes[5].term == 2
    while nodes[5].log.last_index < idx_fig8:
        nodes[5].propose("pad_term2")
    nodes[5].propose("x=2_term2")
    # Drop S5's outbound replication so {S3, S4} never receive S5's term-2 entries
    nodes[5].msgs.clear()

    # (c) Term 3: S1 steps to term 2 (learning of term 2) and campaigns in term 3, winning votes
    # from {S1, S2, S3} (since S1's log last=(idx_fig8, term=1) is longer than S3's last_index!).
    nodes[1]._become_follower(2, None)
    nodes[1].campaign()
    isolate_4_5 = {(i, j) for i in (1, 2, 3) for j in (4, 5)} | {
        (j, i) for i in (1, 2, 3) for j in (4, 5)
    }
    _deliver_all(nodes, blocked=isolate_4_5, drop_types={MsgType.APPEND})
    assert nodes[1].role is StateRole.LEADER and nodes[1].term == 3

    # Trigger heartbeats so S1 probes S2 & S3 and replicates ONLY up to idx_fig8 (term 1).
    for _ in range(3):
        nodes[1].tick()
    for _ in range(8):
        for nid in (1, 2, 3):
            if not nodes[nid].has_ready():
                continue
            rd = nodes[nid].ready()
            nodes[nid].advance(rd)
            for m in rd.messages:
                if (m.src, m.dst) in isolate_4_5:
                    continue
                if (
                    m.type is MsgType.APPEND
                    and any(e.index > idx_fig8 for e in m.entries)
                    and nodes[m.dst].log.match_term(m.index, m.log_term)
                ):
                    continue
                nodes[m.dst].step(m)

    # Verify idx_fig8 (term 1) is now stored on a majority {S1, S2, S3}!
    assert all(nodes[i].log.term(idx_fig8) == 1 for i in (1, 2, 3))
    prematurely_committed = nodes[1].log.committed >= idx_fig8

    # (d) Term 4: S1 crashes; S5 campaigns in term 4 (last_term=2 > term=1 on S2,S3,S4!) and wins!
    from helios_consensus.core.log import LogSafetyError

    nodes[5]._become_follower(3, None)
    nodes[5].campaign()
    isolate_1 = {(1, j) for j in (2, 3, 4, 5)} | {(j, 1) for j in (2, 3, 4, 5)}
    try:
        _deliver_all(nodes, blocked=isolate_1)
        assert nodes[5].role is StateRole.LEADER and nodes[5].term == 4
        nodes[5].propose("finalize_term4")
        for _ in range(3):
            nodes[5].tick()
        _deliver_all(nodes, blocked=isolate_1)
    except LogSafetyError:
        return True, ["LogSafetyError: committed entry overwritten"]

    final_terms = [f"n{i}:t{nodes[i].log.term(idx_fig8)}" for i in (2, 3, 4, 5)]
    overwritten = prematurely_committed and all(
        nodes[i].log.term(idx_fig8) == 2 for i in (2, 3, 4, 5)
    )
    return overwritten, final_terms


def test_ongaro_figure_8_prior_term_commit_rule_deterministic() -> None:
    """Verify §5.4.2 prevents Figure 8 safety violation while ``commit_prior_term`` triggers it."""
    safe_overwritten, safe_terms = _run_figure_8_scenario(buggy_commit_prior_term=False)
    assert safe_overwritten is False
    assert all(t.endswith(":t2") for t in safe_terms)

    buggy_overwritten, _ = _run_figure_8_scenario(buggy_commit_prior_term=True)
    assert buggy_overwritten is True


def test_prevote_prevents_partitioned_minority_term_inflation() -> None:
    nodes = _make_cluster([1, 2, 3], RaftConfig(election_tick=10, heartbeat_tick=2, pre_vote=True))
    nodes[1].campaign()
    _deliver_all(nodes)
    assert nodes[1].role is StateRole.LEADER and nodes[1].term == 1

    # Isolate node 3 completely and tick 200 times
    cut_3 = {(3, 1), (1, 3), (3, 2), (2, 3)}
    for _ in range(200):
        for i in (1, 2, 3):
            nodes[i].tick()
        _deliver_all(nodes, blocked=cut_3)

    # Node 3 stays at term 1 in PRE_CANDIDATE; healing does NOT dethrone node 1!
    assert nodes[3].term == 1 and nodes[3].role is StateRole.PRE_CANDIDATE
    for _ in range(5):
        for i in (1, 2, 3):
            nodes[i].tick()
        _deliver_all(nodes)
    assert nodes[1].role is StateRole.LEADER and nodes[1].term == 1


def test_check_quorum_steps_down_isolated_leader_and_stickiness_blocks_spoofed_votes() -> None:
    nodes = _make_cluster(
        [1, 2, 3], RaftConfig(election_tick=10, heartbeat_tick=2, check_quorum=True)
    )
    nodes[1].campaign()
    _deliver_all(nodes)
    assert nodes[1].role is StateRole.LEADER

    # While node 2 recently heard from leader 1, an outsider's higher-term VOTE is ignored!
    nodes[2].step(Message(type=MsgType.VOTE, src=3, dst=2, term=99, index=10, log_term=99))
    assert nodes[2].term == 1 and nodes[2].lead == 1

    # Isolate leader 1 from {2, 3}; after two CheckQuorum windows (2 * election_tick), leader 1 steps down
    cut_1 = {(1, 2), (2, 1), (1, 3), (3, 1)}
    for _ in range(25):
        nodes[1].tick()
        _deliver_all(nodes, blocked=cut_1)
    assert nodes[1].role.value == StateRole.FOLLOWER.value


def test_read_index_leadership_transfer_and_snapshot_install_retry() -> None:
    nodes = _make_cluster([1, 2, 3], RaftConfig(election_tick=10, heartbeat_tick=2))
    with pytest.raises(NotLeaderError):
        nodes[2].propose("nope")

    nodes[1].campaign()
    _deliver_all(nodes)
    assert nodes[1].role is StateRole.LEADER

    with pytest.raises(ValueError, match="positive"):
        nodes[1].read_index(0)

    for k in range(10):
        nodes[1].propose(f"v{k}")
    # Isolate node 3 so it misses all 10 entries, then compact node 1's log!
    cut_3 = {(1, 3), (3, 1), (2, 3), (3, 2)}
    _deliver_all(nodes, blocked=cut_3)
    nodes[1].compact(nodes[1].log.applied, data="snap_at_11")

    # Drop the first SNAPSHOT message; verify leader retries after election_tick and catches node 3 up
    _deliver_all(nodes, drop_types={MsgType.SNAPSHOT})
    for _ in range(12):
        for n in nodes.values():
            n.tick()
        _deliver_all(nodes)
    assert nodes[3].log.committed == nodes[1].log.committed

    # ReadIndex succeeds with quorum heartbeat acks
    nodes[1].read_index(55)
    for _ in range(3):
        for n in nodes.values():
            n.tick()
        for n in nodes.values():
            if n.has_ready():
                rd = n.ready()
                n.advance(rd)
                for m in rd.messages:
                    nodes[m.dst].step(m)
                if n.id == 1 and rd.read_states:
                    assert rd.read_states[0].request_id == 55

    # Leadership transfer from 1 -> 2 blocks proposals during transfer and elects 2 immediately
    with pytest.raises(ValueError, match="cannot transfer"):
        nodes[1].transfer_leadership(1)
    nodes[1].transfer_leadership(2)
    with pytest.raises(ProposalDroppedError, match="transfer"):
        nodes[1].propose("blocked_during_transfer")
    _deliver_all(nodes)
    assert nodes[2].role is StateRole.LEADER and nodes[2].term == 2
    assert nodes[2].status()["role"] == "LEADER"
    assert "RaftNode(" in repr(nodes[2])
    with pytest.raises(ValueError, match="delivered to"):
        nodes[2].step(Message(type=MsgType.HEARTBEAT, src=1, dst=3, term=2))
