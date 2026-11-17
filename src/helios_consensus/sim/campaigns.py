"""Reproducible Jepsen fault-injection campaigns and 7-mutant bug-killing suite."""

from __future__ import annotations

import time

from helios_consensus.core.node import RaftConfig
from helios_consensus.sim.cluster import Fault, SimConfig, run_simulation
from helios_consensus.types import (
    JepsenCampaignReport,
    JepsenSeedSummary,
    MutantCaseResult,
    MutantSuiteReport,
)


def run_jepsen_campaign(
    *,
    num_seeds: int = 20,
    start_seed: int = 1,
    ticks: int = 1000,
    settle_ticks: int = 240,
    use_segmented_wal: bool = True,
) -> JepsenCampaignReport:
    """Execute ``num_seeds`` deterministic Jepsen runs with full fault & WAL coverage."""
    t0 = time.perf_counter()
    summaries: list[JepsenSeedSummary] = []
    tot_ok = 0
    tot_indet = 0
    tot_elections = 0
    tot_snaps = 0
    tot_reconfigs = 0
    tot_crashes = 0
    tot_lin_states = 0
    safe_count = 0
    live_count = 0

    for seed in range(start_seed, start_seed + num_seeds):
        cfg = SimConfig(
            seed=seed,
            nodes=5,
            ticks=ticks,
            settle_ticks=settle_ticks,
            use_segmented_wal=use_segmented_wal,
            sync_wal=True,
        )
        rep = run_simulation(cfg)
        if rep.safe:
            safe_count += 1
        if rep.liveness_ok:
            live_count += 1
        tot_ok += rep.ops_ok
        tot_indet += rep.ops_indeterminate
        tot_elections += rep.elections
        tot_snaps += rep.snapshots_taken + rep.snapshots_installed
        tot_reconfigs += rep.membership_changes
        tot_crashes += rep.crashes
        tot_lin_states += rep.linearizability.states_explored
        summaries.append(
            JepsenSeedSummary(
                seed=seed,
                safe=rep.safe,
                linearizable=rep.linearizability.ok,
                liveness_ok=rep.liveness_ok,
                ops_ok=rep.ops_ok,
                ops_indeterminate=rep.ops_indeterminate,
                elections=rep.elections,
                snapshots=rep.snapshots_taken,
                membership_changes=rep.membership_changes,
                crashes=rep.crashes,
                fingerprint=rep.fingerprint,
                violations=[f"{v.invariant}@t={v.tick}: {v.detail}" for v in rep.violations[:3]],
            )
        )

    elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 2)
    return JepsenCampaignReport(
        seeds_executed=num_seeds,
        safe_seeds=safe_count,
        liveness_recovered_seeds=live_count,
        segmented_wal_enabled=use_segmented_wal,
        total_committed_ops=tot_ok,
        total_indeterminate_ops=tot_indet,
        total_elections=tot_elections,
        total_snapshots=tot_snaps,
        total_membership_changes=tot_reconfigs,
        total_crashes_injected=tot_crashes,
        total_wgl_states_explored=tot_lin_states,
        elapsed_ms=elapsed_ms,
        seed_summaries=summaries,
    )


def _search_mutant(
    mutant_id: str,
    description: str,
    max_seed: int,
    builder: object,
) -> MutantCaseResult:
    for seed in range(1, max_seed + 1):
        cfg: SimConfig = builder(seed)  # type: ignore[operator]
        rep = run_simulation(cfg)
        if not rep.safe:
            first_detail = (
                rep.violations[0].detail
                if rep.violations
                else f"non-linearizable register history on keys {rep.linearizability.failing_keys}"
            )
            return MutantCaseResult(
                mutant_id=mutant_id,
                description=description,
                caught=True,
                witness_seed=seed,
                detected_by=rep.detected_by,
                witness_detail=first_detail,
            )
    return MutantCaseResult(
        mutant_id=mutant_id,
        description=description,
        caught=False,
        witness_seed=None,
        detected_by=[],
        witness_detail="not triggered within seed budget",
    )


def run_mutant_killing_suite() -> MutantSuiteReport:
    """Prove the test harness catches all 7 classic Raft protocol & WAL durability bugs."""
    t0 = time.perf_counter()
    cases: list[MutantCaseResult] = [
        _search_mutant(
            "double_vote",
            "Follower grants more than one vote in the same term (§5.2 violation)",
            30,
            lambda s: SimConfig(seed=s, ticks=600, settle_ticks=100, nemesis_interval=45).with_bugs(
                "double_vote"
            ),
        ),
        _search_mutant(
            "no_election_restriction",
            "Voter ignores candidate log up-to-date check (§5.4.1 violation)",
            30,
            lambda s: SimConfig(seed=s, ticks=600, settle_ticks=100, nemesis_interval=45).with_bugs(
                "no_election_restriction"
            ),
        ),
        _search_mutant(
            "no_truncate_on_conflict",
            "Follower overwrites conflicting entry without truncating stale suffix (§5.3 violation)",
            30,
            lambda s: SimConfig(seed=s, ticks=600, settle_ticks=100, nemesis_interval=45).with_bugs(
                "no_truncate_on_conflict"
            ),
        ),
        _search_mutant(
            "stale_read",
            "Partitioned old leader serves ReadIndex without heartbeat quorum confirmation (§8 violation)",
            40,
            lambda s: SimConfig(seed=s, ticks=750, settle_ticks=120, nemesis_interval=45).with_bugs(
                "stale_read"
            ),
        ),
        _search_mutant(
            "commit_prior_term",
            "Leader omits election no-op and commits prior-term entries by counting replicas (Figure 8)",
            60,
            lambda s: SimConfig(
                seed=s,
                nodes=5,
                ticks=800,
                settle_ticks=100,
                min_delay=1,
                max_delay=8,
                nemesis_interval=12,
                faults=(Fault.CRASH, Fault.PARTITION, Fault.ISOLATE_LEADER, Fault.HEAL),
                raft=RaftConfig(
                    election_tick=14,
                    heartbeat_tick=3,
                    max_entries_per_msg=1,
                    pre_vote=False,
                    check_quorum=False,
                    bugs=frozenset({"commit_prior_term"}),
                ),
            ),
        ),
        _search_mutant(
            "volatile_vote",
            "Node replies to RequestVote before persisting votedFor across crash/restart",
            160,
            lambda s: SimConfig(
                seed=s,
                nodes=3,
                ticks=850,
                settle_ticks=100,
                min_delay=1,
                max_delay=8,
                nemesis_interval=12,
                faults=(Fault.CRASH, Fault.PARTITION, Fault.ISOLATE_LEADER, Fault.HEAL),
                raft=RaftConfig(
                    election_tick=14,
                    heartbeat_tick=3,
                    max_entries_per_msg=1,
                    pre_vote=False,
                    check_quorum=False,
                    bugs=frozenset({"volatile_vote"}),
                ),
            ),
        ),
        _search_mutant(
            "unsynced_wal_power_cut",
            "SegmentedWAL skips fsync(); power-cut crash loses acknowledged votes & entries in OS cache",
            80,
            lambda s: SimConfig(
                seed=s,
                nodes=3,
                ticks=750,
                settle_ticks=100,
                min_delay=1,
                max_delay=7,
                nemesis_interval=10,
                use_segmented_wal=True,
                sync_wal=False,
                faults=(Fault.CRASH, Fault.PARTITION, Fault.HEAL),
                raft=RaftConfig(
                    election_tick=14,
                    heartbeat_tick=3,
                    pre_vote=False,
                    check_quorum=False,
                ),
            ),
        ),
    ]
    caught = sum(1 for c in cases if c.caught)
    return MutantSuiteReport(
        total_mutants=len(cases),
        mutants_killed=caught,
        kill_rate_pct=round((caught / len(cases)) * 100.0, 2),
        elapsed_ms=round((time.perf_counter() - t0) * 1000.0, 2),
        cases=cases,
    )
