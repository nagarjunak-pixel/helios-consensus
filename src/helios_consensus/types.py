"""Typed domain models for HeliosConsensus Raft DST, TLA+ Model Checker, HotStuff BFT, and Kernels."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class RaftTimelineEvent(BaseModel):
    model_config = ConfigDict(frozen=True)

    tick: int
    event_type: str
    detail: str


class RaftDSTReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    cluster_size: int
    total_ticks: int
    total_messages_routed: int
    dropped_partition_messages: int
    elections_completed: int
    committed_log_entries: int
    election_safety_verified: bool
    log_matching_verified: bool
    linearizable_state_hash: str
    timeline: list[RaftTimelineEvent]


class ModelCheckerReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    states_explored: int
    transitions_checked: int
    max_bfs_depth: int
    election_safety_holds: bool
    log_matching_holds: bool
    leader_completeness_holds: bool
    mutant_bug_caught: bool
    mutant_counterexample_steps: list[str]


class HotStuffBFTReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    validators_n: int
    byzantine_faults_f: int
    quorum_threshold_2f1: int
    honest_quorum_overlap_min: int
    decided_3chain_blocks: int
    equivocation_attempts_rejected: int
    linear_authenticator_complexity: str
    safety_certified: bool


class JumpHashWALReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    kernel_backend: str
    keys_routed: int
    buckets_before: int
    buckets_after: int
    theoretical_remapped_fraction: float
    measured_remapped_fraction: float
    max_bucket_load_skew_pct: float
    cpp_vs_python_hash_mismatches: int
    wal_frames_verified: int
    bitflip_corruptions_caught: int


class ConsensusIncidentOutcome(BaseModel):
    model_config = ConfigDict(frozen=True)

    incident_id: str
    title: str
    expected_safe_quorum_or_fencing: int
    raw_llm_value: int | None
    raw_llm_safe: bool
    model_checked_value: int | None
    model_checked_safe: bool
    turns_used: int
    latency_ms: float


class RealConsensusAuditReport(BaseModel):
    model_config = ConfigDict(frozen=True)

    model_name: str
    num_incidents: int
    raw_safe_count: int
    raw_safe_pct: float
    verified_safe_count: int
    verified_safe_pct: float
    safety_lift_pct: float
    zero_split_brain_leakage: bool
    outcomes: list[ConsensusIncidentOutcome]
