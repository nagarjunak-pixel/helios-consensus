"""Unit and Hypothesis property tests for Raft DST partition safety and TLA+ BFS model checker."""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from helios_consensus.raft.engine import RaftClusterSimulator, run_raft_dst_partition_benchmark
from helios_consensus.verification.model_checker import explore_consensus_state_space


def test_raft_cluster_validation_and_deterministic_split_brain_recovery() -> None:
    with pytest.raises(ValueError, match="odd"):
        RaftClusterSimulator(cluster_size=4)
    with pytest.raises(ValueError, match="odd"):
        RaftClusterSimulator(cluster_size=1)

    sim = RaftClusterSimulator(cluster_size=3)
    # Before any leader is elected, client command returns False cleanly
    assert sim.submit_client_command("NOOP") is False

    rep5 = run_raft_dst_partition_benchmark(cluster_size=5)
    assert rep5.election_safety_verified is True
    assert rep5.log_matching_verified is True
    assert rep5.committed_log_entries == 4
    assert rep5.dropped_partition_messages > 0

    rep7 = run_raft_dst_partition_benchmark(cluster_size=7)
    assert rep7.election_safety_verified is True
    assert rep7.log_matching_verified is True


@settings(max_examples=20, deadline=None)
@given(
    split_tick=st.integers(min_value=8, max_value=16),
    heal_tick=st.integers(min_value=12, max_value=22),
)
def test_hypothesis_randomized_partition_schedules_preserve_raft_safety(
    split_tick: int, heal_tick: int
) -> None:
    sim = RaftClusterSimulator(cluster_size=5)
    e1, l1 = sim.step_ticks(split_tick)
    assert e1 and l1
    sim.submit_client_command("CMD_ALPHA")
    sim.set_partition({0, 1}, {2, 3, 4})
    sim.submit_client_command("ISOLATED_STALE", target_leader_id=0)
    e2, l2 = sim.step_ticks(heal_tick)
    assert e2 and l2
    sim.heal_partition()
    e3, l3 = sim.step_ticks(12)
    assert e3 and l3


def test_tla_bfs_model_checker_proves_raft_and_catches_mutant() -> None:
    rep = explore_consensus_state_space(max_term=2, max_log_len=2)
    assert rep.states_explored > 500
    assert rep.election_safety_holds is True
    assert rep.log_matching_holds is True
    assert rep.leader_completeness_holds is True
    assert rep.mutant_bug_caught is True
    assert len(rep.mutant_counterexample_steps) == 4
