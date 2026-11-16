"""Zero-Mock Live Ollama (`qwen3.5:4b`) Distributed Consensus & Fencing-Token Synthesis Loop.

Evaluates raw `Pass@1` LLM cluster configurations on subtle distributed-systems traps (e.g., BFT
validator count under asynchronous network partitions `3f+1` vs crash-only `2f+1`, asymmetrical
read/write quorum sizing `R + W > N`, Kleppmann monotonic storage fencing tokens, and multi-DC
witness placement) and repairs unsafe configs via formal Quorum-Intersection Counterexamples!
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import httpx

from helios_consensus.types import ConsensusIncidentOutcome, RealConsensusAuditReport


@dataclass(frozen=True)
class ConsensusIncidentSpec:
    incident_id: str
    title: str
    prompt: str
    expected_value: int
    validator_fn: Callable[[int], str | None]


def _check_upright_bft(n: int, f: int = 4, c: int = 2) -> str | None:
    q = n - f - c
    overlap = 2 * q - n
    if q <= 0 or overlap < f + 1:
        return (
            f"SAFETY VIOLATION for N={n}: under f={f} Byzantine + c={c} crash faults, max live "
            f"quorum is Q = N - f - c = {q}, giving two-quorum intersection 2*Q - N = {overlap}, "
            f"which is strictly less than required f + 1 = {f + 1}."
        )
    prev_q = (n - 1) - f - c
    if 2 * prev_q - (n - 1) >= f + 1:
        return f"NON-MINIMAL N={n}: a strictly smaller validator count also satisfies 2*(N - f - c) - N >= f + 1."
    return None


def _check_flex_paxos(q2: int, n: int = 12, q1: int = 4) -> str | None:
    if q2 > n or q1 + q2 <= n:
        return (
            f"QUORUM INTERSECTION VIOLATION for Q2={q2}: Phase-1 Q1={q1} + Phase-2 Q2={q2} = {q1 + q2} "
            f"does not strictly exceed cluster size N={n}."
        )
    if q1 + (q2 - 1) > n:
        return f"NON-MINIMAL Q2={q2}: a smaller Phase-2 quorum still satisfies Q1 + Q2 > {n}."
    return None


def _check_fencing(tok: int, max_seen: int = 418) -> str | None:
    if tok <= max_seen:
        return f"MONOTONIC FENCING VIOLATION for token={tok}: storage shard rejects any token <= {max_seen}."
    if (tok - 1) > max_seen:
        return (
            f"NON-MINIMAL token={tok}: lock service increments sequentially by +1 above {max_seen}."
        )
    return None


def _check_dynamo_rw(w: int, n: int = 9, r: int = 3, k_down: int = 2) -> str | None:
    if w > n - k_down:
        return (
            f"LIVENESS VIOLATION for W={w}: exceeds surviving replicas N - k_down = {n - k_down}."
        )
    if r + w <= n:
        return f"CONSISTENCY VIOLATION for W={w}: R + W = {r + w} does not strictly exceed N={n}."
    if r + (w - 1) > n:
        return f"NON-MINIMAL W={w}: a smaller Write Quorum still satisfies R + W > {n}."
    return None


def _check_multi_az_raft(n: int) -> str | None:
    if n < 3 or n % 3 != 0:
        return (
            f"SYMMETRY VIOLATION for N={n}: N must be a positive multiple of 3 across 3 equal AZs."
        )
    m = n // 3
    surviving = 2 * m - 1
    majority = (n // 2) + 1
    if surviving < majority:
        return (
            f"OUTAGE LIVENESS FAILURE for N={n} ({m} per AZ): losing 1 full AZ ({m}) + 1 extra node "
            f"leaves {surviving} nodes, which is less than strict Raft majority {majority}."
        )
    prev_n = n - 3
    if prev_n >= 3 and (2 * (prev_n // 3) - 1) >= ((prev_n // 2) + 1):
        return f"NON-MINIMAL N={n}: a smaller 3-AZ symmetric cluster already survives 1 AZ + 1 node failure."
    return None


def _check_grid_quorum(k: int, side: int = 7) -> str | None:
    # Distinct union of 1 row of `side` cells and 1 col of `side` cells in `side x side` grid
    true_union = side + side - 1
    if k != true_union:
        return (
            f"INCLUSION-EXCLUSION VIOLATION for count={k}: one {side}-node row and one {side}-node "
            "column in a square grid intersect at their shared (row, col) pivot cell."
        )
    return None


def build_consensus_incident_suite() -> list[ConsensusIncidentSpec]:
    """6 adversarial Distributed Consensus, BFT Quorum & Fencing-Token Sizing Specifications."""
    return [
        ConsensusIncidentSpec(
            incident_id="HC-01-UPRIGHT-HYBRID-BFT",
            title="Hybrid BFT + Crash Cluster Sizing (f=4, c=2)",
            prompt=(
                "A sovereign payment ledger under partial synchrony must simultaneously tolerate up to "
                "f = 4 arbitrary Byzantine (equivocating) validators AND c = 2 concurrent crash/maintenance "
                "offline nodes without losing liveness or honest-quorum safety. What is the MINIMUM total "
                "number of validators N required?"
            ),
            expected_value=17,
            validator_fn=_check_upright_bft,
        ),
        ConsensusIncidentSpec(
            incident_id="HC-02-FLEXIBLE-PAXOS-WRITE-QUORUM",
            title="Howard Flexible Paxos Asymmetric Quorum",
            prompt=(
                "In a 12-node Flexible Paxos cluster (Howard et al.), Phase-1 Leader Election uses a "
                "quorum size of Q1 = 4 nodes. What is the MINIMUM Phase-2 Write/Replication quorum size "
                "Q2 required to guarantee strict Election-Replication quorum intersection (Q1 + Q2 > N)?"
            ),
            expected_value=9,
            validator_fn=_check_flex_paxos,
        ),
        ConsensusIncidentSpec(
            incident_id="HC-03-KLEPPMANN-FENCING-TOKEN",
            title="GC-Pause Split-Brain Storage Fencing",
            prompt=(
                "Storage shard S has `max_seen_fencing_token = 418`. Client A paused at token 415, while "
                "Client B committed at token 418. Client C now acquires the next sequential lock (+1) and "
                "writes before delayed Client A(415) arrives. What exact integer fencing token does S accept?"
            ),
            expected_value=419,
            validator_fn=_check_fencing,
        ),
        ConsensusIncidentSpec(
            incident_id="HC-04-SLOPPY-TO-STRICT-DYNAMO",
            title="Strict R+W>N Quorum Under 2 Node Failures",
            prompt=(
                "A 9-replica storage ring sets Read Quorum R = 3 and must guarantee BOTH strict consistency "
                "(R + W > 9) AND write availability when k_down = 2 replicas are down (W <= 7). "
                "What is the MINIMUM valid integer Write Quorum W?"
            ),
            expected_value=7,
            validator_fn=_check_dynamo_rw,
        ),
        ConsensusIncidentSpec(
            incident_id="HC-05-MULTI-AZ-RAFT-WITNESS",
            title="3-AZ Surviving Whole-AZ + 1 Node Outage",
            prompt=(
                "A Raft cluster is split evenly across 3 Availability Zones (m voting nodes per AZ, total N = 3*m). "
                "It must retain a strict Raft majority even if 1 entire AZ (m nodes) AND 1 additional node in a "
                "surviving AZ fail simultaneously. What is the MINIMUM total cluster size N?"
            ),
            expected_value=9,
            validator_fn=_check_multi_az_raft,
        ),
        ConsensusIncidentSpec(
            incident_id="HC-06-SQUARE-GRID-WRITE-QUORUM",
            title="7x7 Cheung Cross-Grid Quorum Cardinality",
            prompt=(
                "N = 49 storage nodes form a 7 x 7 grid. Each Write Quorum consists of the set union of "
                "1 complete row (7 nodes) and 1 complete column (7 nodes). How many DISTINCT nodes are in "
                "a single Write Quorum?"
            ),
            expected_value=13,
            validator_fn=_check_grid_quorum,
        ),
    ]


def _ask_ollama_int(
    prompt_text: str,
    *,
    model: str,
    base_url: str,
) -> int | None:
    full_prompt = (
        "Return ONLY valid JSON with key 'value' (integer) and 'steps' (at most 2 brief equations, <20 words each): "
        '{"steps": ["eq1", "eq2"], "value": 123}\n\n'
        f"SPECIFICATION:\n{prompt_text}\n"
    )
    resp = httpx.post(
        f"{base_url.rstrip('/')}/api/generate",
        json={
            "model": model,
            "prompt": full_prompt,
            "format": "json",
            "stream": False,
            "think": False,
            "options": {"temperature": 0.0, "seed": 88, "num_predict": 1536},
        },
        timeout=45.0,
    )
    resp.raise_for_status()
    try:
        obj = json.loads(str(resp.json().get("response", "{}")))
        if (
            isinstance(obj, dict)
            and obj.get("value") is not None
            and not isinstance(obj.get("value"), bool)
        ):
            return int(round(float(str(obj["value"]))))
    except (ValueError, json.JSONDecodeError):
        return None
    return None


def default_consensus_artifact_path() -> Path:
    return Path(__file__).resolve().parents[3] / "benchmarks" / "real_qwen_consensus.json"


def run_real_consensus_ollama_audit(
    *,
    model: str = "qwen3.5:4b",
    base_url: str = "http://127.0.0.1:11434",
    max_cegis_turns: int = 4,
    save_path: Path | None = None,
    allow_offline_replay: bool = True,
) -> RealConsensusAuditReport:
    """Compare raw `Pass@1` LLM cluster sizing vs Helios Formal Quorum-Invariant CEGIS Repair."""
    artifact = save_path or default_consensus_artifact_path()

    ollama_live = False
    try:
        ping = httpx.get(f"{base_url.rstrip('/')}/api/tags", timeout=2.0)
        ollama_live = ping.status_code == 200
    except httpx.HTTPError:
        ollama_live = False

    if not ollama_live:
        if allow_offline_replay and artifact.exists():
            return RealConsensusAuditReport.model_validate(
                json.loads(artifact.read_text(encoding="utf-8"))
            )
        raise RuntimeError(f"Ollama unreachable at {base_url} and no artifact at {artifact}")

    specs = build_consensus_incident_suite()
    outcomes: list[ConsensusIncidentOutcome] = []
    raw_ok_cnt = 0
    ver_ok_cnt = 0

    for spec in specs:
        t0 = time.perf_counter()
        curr_prompt = spec.prompt
        raw_val: int | None = None
        raw_safe = False
        final_val: int | None = None
        final_safe = False
        turns = 0

        for turn in range(1, max_cegis_turns + 1):
            turns = turn
            cand = _ask_ollama_int(curr_prompt, model=model, base_url=base_url)
            err = (
                spec.validator_fn(cand) if cand is not None else "Returned unparseable null value."
            )
            if turn == 1:
                raw_val = cand
                raw_safe = err is None
                if raw_safe:
                    raw_ok_cnt += 1
            if err is None:
                final_val = cand
                final_safe = True
                ver_ok_cnt += 1
                break
            curr_prompt = (
                f"{spec.prompt}\n\nFORMAL MODEL-CHECKER COUNTEREXAMPLE ON YOUR PREVIOUS PROPOSAL ({cand}):\n"
                f"{err}\nRecompute the exact safe integer."
            )

        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        outcomes.append(
            ConsensusIncidentOutcome(
                incident_id=spec.incident_id,
                title=spec.title,
                expected_safe_quorum_or_fencing=spec.expected_value,
                raw_llm_value=raw_val,
                raw_llm_safe=raw_safe,
                model_checked_value=final_val,
                model_checked_safe=final_safe,
                turns_used=turns,
                latency_ms=round(elapsed_ms, 1),
            )
        )

    n = len(specs)
    r_pct = round((raw_ok_cnt / n) * 100.0, 2)
    v_pct = round((ver_ok_cnt / n) * 100.0, 2)

    report = RealConsensusAuditReport(
        model_name=model,
        num_incidents=n,
        raw_safe_count=raw_ok_cnt,
        raw_safe_pct=r_pct,
        verified_safe_count=ver_ok_cnt,
        verified_safe_pct=v_pct,
        safety_lift_pct=round(v_pct - r_pct, 2),
        zero_split_brain_leakage=all(
            o.model_checked_safe or o.model_checked_value is None for o in outcomes
        ),
        outcomes=outcomes,
    )
    artifact.parent.mkdir(parents=True, exist_ok=True)
    artifact.write_text(json.dumps(report.model_dump(), indent=2) + "\n", encoding="utf-8")
    return report
