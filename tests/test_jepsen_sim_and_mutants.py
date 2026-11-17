"""Jepsen fault-injection campaign, deterministic replay, disjoint Joint Consensus & 7-mutant tests."""

from __future__ import annotations

from helios_consensus.check.linearizability import OpKind
from helios_consensus.sim.campaigns import run_jepsen_campaign, run_mutant_killing_suite
from helios_consensus.sim.cluster import Fault, SimCluster, SimConfig, run_simulation


def test_deterministic_seed_replay_is_bit_identical() -> None:
    cfg = SimConfig(
        seed=17,
        nodes=5,
        ticks=650,
        settle_ticks=180,
        use_segmented_wal=True,
        sync_wal=True,
    )
    r1 = run_simulation(cfg)
    r2 = run_simulation(cfg)
    assert r1.safe is True
    assert r1.liveness_ok is True
    assert r1.fingerprint == r2.fingerprint
    assert r1.ops_ok == r2.ops_ok
    assert r1.ops_indeterminate == r2.ops_indeterminate
    assert r1.event_log == r2.event_log


def test_jepsen_campaign_all_seeds_safe_and_linearizable() -> None:
    rep = run_jepsen_campaign(
        num_seeds=12,
        start_seed=1,
        ticks=700,
        settle_ticks=180,
        use_segmented_wal=True,
    )
    assert rep.seeds_executed == 12
    assert rep.safe_seeds == 12
    assert rep.liveness_recovered_seeds == 12
    assert rep.total_committed_ops > 2_000
    assert rep.total_elections >= 12
    assert all(s.safe and s.linearizable and s.liveness_ok for s in rep.seed_summaries)


def test_seven_mutant_protocol_and_wal_killing_suite() -> None:
    suite = run_mutant_killing_suite()
    assert suite.total_mutants == 7
    assert suite.mutants_killed == 7
    assert suite.kill_rate_pct == 100.0
    by_id = {c.mutant_id: c for c in suite.cases}
    expected_ids = {
        "double_vote",
        "no_election_restriction",
        "no_truncate_on_conflict",
        "stale_read",
        "commit_prior_term",
        "volatile_vote",
        "unsynced_wal_power_cut",
    }
    assert set(by_id) == expected_ids
    for mutant_id, case in by_id.items():
        assert case.caught is True, f"mutant {mutant_id} escaped detection"
        assert case.witness_seed is not None
        assert len(case.detected_by) >= 1


def _step_cluster(cluster: SimCluster, ticks: int, *, issue_ops: bool = True) -> None:
    for _ in range(ticks):
        cluster.now += 1
        cluster._deliver()
        cluster._tick_nodes()
        cluster._process_all_ready()
        cluster._clients_act(issue=issue_ops)
        cluster._expire_ops()
        cluster._timed_recoveries()
        live_rafts = [n.raft for n in cluster.live_nodes() if n.raft is not None]
        cluster.observer.check_roles(live_rafts, cluster.now)
        if cluster.now % 10 == 0:
            cluster.observer.check_log_matching(live_rafts, cluster.now)


def test_disjoint_joint_consensus_migration_under_client_load() -> None:
    """Migrate a live 3-voter cluster {1, 2, 3} to a completely disjoint {4, 5, 6} voter set."""
    cfg = SimConfig(
        seed=42,
        nodes=3,
        ticks=500,
        settle_ticks=150,
        nemesis_interval=10_000,  # manual fault control
        faults=(Fault.HEAL,),
        use_segmented_wal=True,
    )
    cluster = SimCluster(cfg)
    _step_cluster(cluster, 80)
    ldr = cluster.leader()
    assert ldr is not None and ldr.raft is not None
    assert ldr.raft.membership.voters == frozenset({1, 2, 3})

    # Spawn nodes 4, 5, 6 and propose disjoint Joint Consensus C_{old,new} -> C_new
    for nid in (4, 5, 6):
        cluster._create_node(nid)
    cluster.next_node_id = 7
    ldr.raft.propose_joint_reconfig({4, 5, 6})
    cluster._removed_pending |= {1, 2, 3}
    cluster.membership_changes += 1

    _step_cluster(cluster, 260, issue_ops=True)
    _step_cluster(cluster, 90, issue_ops=False)

    rep = cluster._report()
    assert rep.safe is True
    assert rep.linearizability.ok is True
    assert rep.final_voters == [4, 5, 6]
    assert any(
        h.status == "ok" and h.kind is OpKind.WRITE and h.call > 160 for h in cluster.history
    )
