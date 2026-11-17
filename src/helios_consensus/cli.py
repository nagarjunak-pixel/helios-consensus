"""Command-line interface for HeliosConsensus (`helios`)."""

from __future__ import annotations

import json
from pathlib import Path

import typer
import uvicorn
from rich.console import Console
from rich.table import Table

from helios_consensus.api.app import create_app
from helios_consensus.audit.ollama_consensus import run_real_consensus_ollama_audit
from helios_consensus.hotstuff.bft_qc import run_hotstuff_3chain_benchmark
from helios_consensus.kernels.native_ring import benchmark_jump_hash_and_crc32c
from helios_consensus.multiraft import MultiRaftCluster
from helios_consensus.raft.engine import run_raft_dst_partition_benchmark
from helios_consensus.sim.campaigns import run_jepsen_campaign, run_mutant_killing_suite
from helios_consensus.verification.model_checker import explore_consensus_state_space

app = typer.Typer(
    name="helios",
    help="HeliosConsensus: Raft Consensus Engine, Jepsen Fault-Injection Harness, WGL Linearizability Checker & C++20 WAL Runtime.",
    add_completion=False,
)
console = Console()


@app.command("jepsen")
def jepsen_cmd(
    seeds: int = typer.Option(15, "--seeds", "-s", help="Number of deterministic seeds to run"),
    ticks: int = typer.Option(1000, "--ticks", "-t", help="Fault-injection ticks per seed"),
    segmented_wal: bool = typer.Option(
        True, "--segmented-wal/--memory-wal", help="Use C++20 binary SegmentedWAL + power-cut disk"
    ),
    as_json: bool = typer.Option(False, "--json", help="Emit structured JSON report"),
) -> None:
    """Run Jepsen-style fault injection (partitions, bridges, crashes, joint reconfigs, snapshots) & WGL linearizability."""
    rep = run_jepsen_campaign(num_seeds=seeds, ticks=ticks, use_segmented_wal=segmented_wal)
    if as_json:
        typer.echo(json.dumps(rep.model_dump(), indent=2))
        return
    tbl = Table(
        title=f"Jepsen Fault-Injection & Linearizability Campaign (seeds={rep.seeds_executed})"
    )
    tbl.add_column("Metric", style="cyan")
    tbl.add_column("Value", justify="right", style="green")
    tbl.add_row("Safe & Linearizable Seeds", f"{rep.safe_seeds}/{rep.seeds_executed}")
    tbl.add_row(
        "Post-Heal Liveness Recovered", f"{rep.liveness_recovered_seeds}/{rep.seeds_executed}"
    )
    tbl.add_row(
        "Committed / Indeterminate Client Ops",
        f"{rep.total_committed_ops:,} / {rep.total_indeterminate_ops:,}",
    )
    tbl.add_row(
        "Elections / Snapshots / Reconfigs / Crashes",
        f"{rep.total_elections} / {rep.total_snapshots} / {rep.total_membership_changes} / {rep.total_crashes_injected}",
    )
    tbl.add_row("Wing-Gong-Lowe States Explored", f"{rep.total_wgl_states_explored:,}")
    tbl.add_row("Elapsed Wall Time", f"{rep.elapsed_ms:.1f} ms")
    console.print(tbl)


@app.command("kill-mutants")
def kill_mutants_cmd(
    as_json: bool = typer.Option(False, "--json", help="Emit structured JSON report"),
) -> None:
    """Verify the harness catches all 7 classic Raft protocol & unsynced-WAL durability bugs."""
    rep = run_mutant_killing_suite()
    if as_json:
        typer.echo(json.dumps(rep.model_dump(), indent=2))
        return
    tbl = Table(
        title=f"Protocol & Storage Mutant-Killing Suite ({rep.mutants_killed}/{rep.total_mutants} killed)"
    )
    tbl.add_column("Mutant", style="cyan")
    tbl.add_column("Seed", justify="right")
    tbl.add_column("Detected By", style="yellow")
    tbl.add_column("Witness")
    for c in rep.cases:
        tbl.add_row(
            c.mutant_id,
            str(c.witness_seed) if c.witness_seed is not None else "—",
            ", ".join(c.detected_by) if c.detected_by else "MISSED",
            c.witness_detail[:72],
        )
    console.print(tbl)


@app.command("multiraft")
def multiraft_cmd(
    stores: int = typer.Option(5, "--stores", help="Number of physical stores"),
    as_json: bool = typer.Option(False, "--json", help="Emit structured JSON report"),
) -> None:
    """Demonstrate Jump-Hash sharded Multi-Raft ranges, online range split & TimeoutNow rebalancing."""
    mr = MultiRaftCluster(
        num_stores=stores, initial_boundaries=["g", "n", "t"], replicas_per_range=3
    )
    mr.wait_all_leaders()
    for idx, word in enumerate(
        ("alpha", "delta", "echo", "india", "oscar", "victor", "zulu"), start=1
    ):
        mr.put(word, idx * 10)
    mr.step_ticks(3)
    first_rid = mr.locate_range("alpha").range_id
    left, right = mr.split_range(first_rid, "d")
    balanced = mr.rebalance_leaders()
    summary = {
        "stores": stores,
        "ranges": len(mr.ranges),
        "split_left": [left.start_key, left.end_key],
        "split_right": [right.start_key, right.end_key],
        "leaders_per_store": balanced,
        "sample_read_echo": mr.get_linearizable("echo"),
    }
    if as_json:
        typer.echo(json.dumps(summary, indent=2))
        return
    console.print(
        f"[bold cyan]Multi-Raft:[/] {len(mr.ranges)} ranges across {stores} stores | "
        f"split ['', 'g') -> ['', 'd') + ['d', 'g') | leaders/store={balanced}"
    )


@app.command("simulate-raft")
def simulate_raft_cmd(
    nodes: int = typer.Option(5, help="Odd Raft cluster size >= 3"),
    as_json: bool = typer.Option(False, "--json", help="Emit structured JSON report"),
) -> None:
    """Run deterministic Raft split-brain partition & heal simulation."""
    rep = run_raft_dst_partition_benchmark(cluster_size=nodes)
    if as_json:
        typer.echo(json.dumps(rep.model_dump(), indent=2))
        return
    tbl = Table(title=f"Raft Split-Brain Partition Simulation (N={rep.cluster_size})")
    tbl.add_column("Metric", style="cyan")
    tbl.add_column("Value", justify="right", style="green")
    tbl.add_row(
        "Logical Ticks / Messages Routed / Partition-Dropped",
        f"{rep.total_ticks} / {rep.total_messages_routed} / {rep.dropped_partition_messages}",
    )
    tbl.add_row(
        "Leader Elections / Committed Entries",
        f"{rep.elections_completed} / {rep.committed_log_entries}",
    )
    tbl.add_row(
        "Election Safety & Log Matching",
        f"{rep.election_safety_verified} & {rep.log_matching_verified}",
    )
    tbl.add_row("State-Machine SHA-256 Digest", rep.linearizable_state_hash)
    console.print(tbl)


@app.command("model-check")
def model_check_cmd(
    as_json: bool = typer.Option(False, "--json", help="Emit structured JSON report"),
) -> None:
    """Bounded explicit-state BFS model checker over reduced 3-node Raft state space."""
    rep = explore_consensus_state_space()
    if as_json:
        typer.echo(json.dumps(rep.model_dump(), indent=2))
        return
    tbl = Table(title="Bounded Explicit-State Consensus Model Checker")
    tbl.add_column("Property", style="cyan")
    tbl.add_column("Status", justify="right", style="green")
    tbl.add_row(
        "Reachable States / Transitions Checked",
        f"{rep.states_explored:,} / {rep.transitions_checked:,} (depth={rep.max_bfs_depth})",
    )
    tbl.add_row(
        "INV-1 ElectionSafety & INV-2 LogMatching",
        f"{rep.election_safety_holds} & {rep.log_matching_holds}",
    )
    tbl.add_row("INV-3 LeaderCompleteness (§5.4.1 Active)", str(rep.leader_completeness_holds))
    tbl.add_row("Mutant Bug (§5.4.1 Disabled) Counterexample Found", str(rep.mutant_bug_caught))
    console.print(tbl)
    for idx, step in enumerate(rep.mutant_counterexample_steps, start=1):
        console.print(f"  [yellow]Step {idx}:[/] {step}")


@app.command("simulate-hotstuff")
def simulate_hotstuff_cmd(
    byzantine_f: int = typer.Option(2, help="Max Byzantine validators f (N = 3f + 1)"),
    as_json: bool = typer.Option(False, "--json", help="Emit structured JSON report"),
) -> None:
    """Execute pipelined HotStuff 3-Chain BFT finality under equivocating Byzantine replicas."""
    rep = run_hotstuff_3chain_benchmark(byzantine_f=byzantine_f)
    if as_json:
        typer.echo(json.dumps(rep.model_dump(), indent=2))
        return
    tbl = Table(
        title=f"HotStuff 3-Chain BFT Quorum Certificates (N={rep.validators_n}, f={rep.byzantine_faults_f})"
    )
    tbl.add_column("Metric", style="cyan")
    tbl.add_column("Value", justify="right", style="green")
    tbl.add_row(
        "2f+1 Quorum Threshold / Min Honest Overlap",
        f"{rep.quorum_threshold_2f1} / {rep.honest_quorum_overlap_min} honest",
    )
    tbl.add_row(
        "3-Chain Finalized Blocks / Equivocations Rejected",
        f"{rep.decided_3chain_blocks} / {rep.equivocation_attempts_rejected}",
    )
    tbl.add_row("Safety Certified", str(rep.safety_certified))
    console.print(tbl)


@app.command("benchmark-kernels")
def benchmark_kernels_cmd(
    as_json: bool = typer.Option(False, "--json", help="Emit structured JSON report"),
) -> None:
    """Benchmark C++20 Jump Consistent Hash minimal migration & slicing-by-8 CRC32C WAL frames."""
    rep = benchmark_jump_hash_and_crc32c()
    if as_json:
        typer.echo(json.dumps(rep.model_dump(), indent=2))
        return
    tbl = Table(title=f"C++20 Jump-Hash & Binary CRC32C WAL Kernel ({rep.kernel_backend})")
    tbl.add_column("Metric", style="cyan")
    tbl.add_column("Value", justify="right", style="green")
    tbl.add_row(
        "Shard Expansion Remapped Keys (Theoretical vs Measured)",
        f"{rep.theoretical_remapped_fraction:.5f} vs {rep.measured_remapped_fraction:.5f}",
    )
    tbl.add_row("C++20 vs Python Hash Mismatches", str(rep.cpp_vs_python_hash_mismatches))
    tbl.add_row(
        "CRC32C Single-Bit WAL Corruptions Caught",
        f"{rep.bitflip_corruptions_caught}/{rep.wal_frames_verified}",
    )
    console.print(tbl)


@app.command("eval-ollama")
def eval_ollama_cmd(
    model: str = typer.Option("qwen3.5:4b", help="Ollama model tag"),
    live_only: bool = typer.Option(False, "--live-only", help="Require live Ollama inference"),
    save_path: Path | None = typer.Option(None, help="Optional custom output JSON path"),
    as_json: bool = typer.Option(False, "--json", help="Emit structured JSON report"),
) -> None:
    """Evaluate raw Pass@1 vs Quorum-Invariant Guided Repair on local Ollama."""
    rep = run_real_consensus_ollama_audit(
        model=model,
        save_path=save_path,
        allow_offline_replay=not live_only,
    )
    if as_json:
        typer.echo(json.dumps(rep.model_dump(), indent=2))
        return
    tbl = Table(title=f"Quorum & Fencing Parameter Check ({rep.model_name})")
    tbl.add_column("Incident ID", style="cyan")
    tbl.add_column("Safe Spec", justify="right")
    tbl.add_column("Raw Pass@1", justify="right")
    tbl.add_column("Verified", justify="right")
    for o in rep.outcomes:
        r_str = (
            f"[green]{o.raw_llm_value} ✓[/]" if o.raw_llm_safe else f"[red]{o.raw_llm_value} ✗[/]"
        )
        v_str = (
            f"[green]{o.model_checked_value} ✓ (t={o.turns_used})[/]"
            if o.model_checked_safe
            else "[yellow]BLOCKED[/]"
        )
        tbl.add_row(o.incident_id, str(o.expected_safe_quorum_or_fencing), r_str, v_str)
    console.print(tbl)
    console.print(
        f"[bold]Summary:[/] Raw = {rep.raw_safe_count}/{rep.num_incidents} ({rep.raw_safe_pct:.1f}%) "
        f"-> Verified = {rep.verified_safe_count}/{rep.num_incidents} ({rep.verified_safe_pct:.1f}%)"
    )


@app.command("demo")
def demo_cmd(
    as_json: bool = typer.Option(False, "--json", help="Emit unified JSON report"),
) -> None:
    """Run Jepsen fault-injection campaign, mutant-killing suite, BFS checker, HotStuff & C++20 WAL checks."""
    jepsen = run_jepsen_campaign(num_seeds=10, ticks=800, settle_ticks=200, use_segmented_wal=True)
    r = run_raft_dst_partition_benchmark()
    m = explore_consensus_state_space()
    h = run_hotstuff_3chain_benchmark()
    k = benchmark_jump_hash_and_crc32c()
    payload = {
        "jepsen": jepsen.model_dump(),
        "raft_dst": r.model_dump(),
        "model_checker": m.model_dump(),
        "hotstuff_bft": h.model_dump(),
        "jump_hash_wal": k.model_dump(),
    }
    if as_json:
        typer.echo(json.dumps(payload, indent=2))
        return
    console.print(
        f"[bold cyan]1. Jepsen Raft + SegmentedWAL:[/] {jepsen.safe_seeds}/{jepsen.seeds_executed} seeds linearizable, "
        f"{jepsen.total_committed_ops:,} ops, {jepsen.total_snapshots} snapshots, {jepsen.total_membership_changes} reconfigs"
    )
    console.print(
        f"[bold magenta]2. Bounded BFS Checker:[/] {m.states_explored:,} states verified, mutant caught={m.mutant_bug_caught}"
    )
    console.print(
        f"[bold green]3. HotStuff 3-Chain BFT:[/] {h.decided_3chain_blocks} blocks decided, {h.equivocation_attempts_rejected} equivocations blocked"
    )
    console.print(
        f"[bold yellow]4. Native C++20 ({k.kernel_backend}):[/] Jump-Hash moved {k.measured_remapped_fraction:.4f} (theo {k.theoretical_remapped_fraction:.4f}), CRC32C {k.bitflip_corruptions_caught}/{k.wal_frames_verified}"
    )


@app.command("serve")
def serve_cmd(
    host: str = typer.Option("127.0.0.1", help="Bind host"),
    port: int = typer.Option(8070, help="Bind port"),
) -> None:
    """Launch the HeliosConsensus FastAPI & Verification Workbench server."""
    uvicorn.run(create_app(), host=host, port=port)


if __name__ == "__main__":
    app()
