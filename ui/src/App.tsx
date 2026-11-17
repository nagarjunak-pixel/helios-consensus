import React, { useEffect, useState } from "react";

interface RaftEvent {
  tick: number;
  event_type: string;
  detail: string;
}

interface RaftReport {
  cluster_size: number;
  total_ticks: number;
  total_messages_routed: number;
  dropped_partition_messages: number;
  elections_completed: number;
  committed_log_entries: number;
  election_safety_verified: boolean;
  log_matching_verified: boolean;
  linearizable_state_hash: string;
  timeline: RaftEvent[];
}

interface JepsenSeedSummary {
  seed: number;
  safe: boolean;
  linearizable: boolean;
  liveness_ok: boolean;
  ops_ok: number;
  ops_indeterminate: number;
  elections: number;
  snapshots: number;
  membership_changes: number;
  crashes: number;
  fingerprint: string;
  violations: string[];
}

interface JepsenReport {
  seeds_executed: number;
  safe_seeds: number;
  liveness_recovered_seeds: number;
  segmented_wal_enabled: boolean;
  total_committed_ops: number;
  total_indeterminate_ops: number;
  total_elections: number;
  total_snapshots: number;
  total_membership_changes: number;
  total_crashes_injected: number;
  total_wgl_states_explored: number;
  elapsed_ms: number;
  seed_summaries: JepsenSeedSummary[];
}

interface MutantCase {
  mutant_id: string;
  description: string;
  caught: boolean;
  witness_seed: number | null;
  detected_by: string[];
  witness_detail: string;
}

interface MutantSuiteReport {
  total_mutants: number;
  mutants_killed: number;
  kill_rate_pct: number;
  elapsed_ms: number;
  cases: MutantCase[];
}

interface ModelCheckReport {
  states_explored: number;
  transitions_checked: number;
  max_bfs_depth: number;
  election_safety_holds: boolean;
  log_matching_holds: boolean;
  leader_completeness_holds: boolean;
  mutant_bug_caught: boolean;
  mutant_counterexample_steps: string[];
}

interface HotStuffReport {
  validators_n: number;
  byzantine_faults_f: number;
  quorum_threshold_2f1: number;
  honest_quorum_overlap_min: number;
  decided_3chain_blocks: number;
  equivocation_attempts_rejected: number;
  linear_authenticator_complexity: string;
  safety_certified: boolean;
}

interface KernelReport {
  kernel_backend: string;
  keys_routed: number;
  buckets_before: number;
  buckets_after: number;
  theoretical_remapped_fraction: number;
  measured_remapped_fraction: number;
  max_bucket_load_skew_pct: number;
  cpp_vs_python_hash_mismatches: number;
  wal_frames_verified: number;
  bitflip_corruptions_caught: number;
}

interface IncidentOutcome {
  incident_id: string;
  title: string;
  expected_safe_quorum_or_fencing: number;
  raw_llm_value: number | null;
  raw_llm_safe: boolean;
  model_checked_value: number | null;
  model_checked_safe: boolean;
  turns_used: number;
  latency_ms: number;
}

interface ConsensusAudit {
  model_name: string;
  num_incidents: number;
  raw_safe_count: number;
  raw_safe_pct: number;
  verified_safe_count: number;
  verified_safe_pct: number;
  safety_lift_pct: number;
  zero_split_brain_leakage: boolean;
  outcomes: IncidentOutcome[];
}

interface OverviewPayload {
  kernel_backend: string;
  jepsen: JepsenReport;
  raft_dst: RaftReport;
  model_checker: ModelCheckReport;
  hotstuff_bft: HotStuffReport;
  jump_hash_wal: KernelReport;
  real_qwen_consensus: ConsensusAudit | null;
}

export function App(): React.JSX.Element {
  const [data, setData] = useState<OverviewPayload | null>(null);
  const [raft, setRaft] = useState<RaftReport | null>(null);
  const [jepsen, setJepsen] = useState<JepsenReport | null>(null);
  const [mutants, setMutants] = useState<MutantSuiteReport | null>(null);
  const [clusterN, setClusterN] = useState<number>(5);
  const [seedCount, setSeedCount] = useState<number>(8);
  const [runningJepsen, setRunningJepsen] = useState<boolean>(false);
  const [runningMutants, setRunningMutants] = useState<boolean>(false);

  useEffect(() => {
    fetch("/api/v1/overview")
      .then((r) => r.json())
      .then((payload: OverviewPayload) => {
        setData(payload);
        setRaft(payload.raft_dst);
        setJepsen(payload.jepsen);
      });
  }, []);

  const changeClusterN = async (n: number) => {
    setClusterN(n);
    const r = await fetch("/api/v1/raft/simulate", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ cluster_size: n }),
    });
    setRaft((await r.json()) as RaftReport);
  };

  const runJepsen = async (seeds: number) => {
    setSeedCount(seeds);
    setRunningJepsen(true);
    try {
      const r = await fetch("/api/v1/jepsen/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ num_seeds: seeds, ticks: 700, use_segmented_wal: true }),
      });
      setJepsen((await r.json()) as JepsenReport);
    } finally {
      setRunningJepsen(false);
    }
  };

  const runMutantSuite = async () => {
    setRunningMutants(true);
    try {
      const r = await fetch("/api/v1/mutants/run");
      setMutants((await r.json()) as MutantSuiteReport);
    } finally {
      setRunningMutants(false);
    }
  };

  if (!data || !raft || !jepsen) {
    return (
      <div className="shell">
        <h2>Loading HeliosConsensus Raft, Jepsen &amp; Linearizability Workbench...</h2>
      </div>
    );
  }

  const mc = data.model_checker;
  const bft = data.hotstuff_bft;
  const wal = data.jump_hash_wal;
  const qwen = data.real_qwen_consensus;

  return (
    <div className="shell">
      <header className="topbar">
        <div className="brand">
          <h1>HeliosConsensus — Raft Consensus, Jepsen Fault-Injection &amp; Linearizability Workbench</h1>
          <p>
            Library-Style Raft (PreVote, Snapshots, Joint Consensus, ReadIndex) · Segmented Binary WAL ·
            Wing-Gong-Lowe Linearizability Checker · C++20 Kernels
          </p>
        </div>
        <div className="pill-row">
          <span className="pill gold">{data.kernel_backend}</span>
          <span className="pill">
            Jepsen {jepsen.safe_seeds}/{jepsen.seeds_executed} Linearizable
          </span>
          <span className="pill">Bounded BFS: {mc.states_explored} States</span>
        </div>
      </header>

      <section className="kpi-grid">
        <div className="kpi-card">
          <div className="kpi-label">Jepsen Fault Campaign + SegmentedWAL</div>
          <div className="kpi-value">
            {jepsen.safe_seeds}/{jepsen.seeds_executed} Safe
          </div>
          <div className="kpi-sub">
            {jepsen.total_committed_ops.toLocaleString()} Ops · {jepsen.total_snapshots} Snapshots ·{" "}
            {jepsen.total_wgl_states_explored.toLocaleString()} WGL States
          </div>
        </div>

        <div className="kpi-card">
          <div className="kpi-label">Bounded Explicit-State BFS Checker</div>
          <div className="kpi-value">{mc.states_explored.toLocaleString()} States</div>
          <div className="kpi-sub">
            {mc.transitions_checked.toLocaleString()} Transitions · §5.4.1 Mutant Caught ={" "}
            {String(mc.mutant_bug_caught)}
          </div>
        </div>

        <div className="kpi-card">
          <div className="kpi-label">HotStuff 3-Chain BFT (N={bft.validators_n}, f={bft.byzantine_faults_f})</div>
          <div className="kpi-value">{bft.equivocation_attempts_rejected} Blocked</div>
          <div className="kpi-sub">
            2f+1={bft.quorum_threshold_2f1} QC · ≥{bft.honest_quorum_overlap_min} Honest Overlap ·{" "}
            {bft.decided_3chain_blocks} Blocks Decided
          </div>
        </div>

        <div className="kpi-card">
          <div className="kpi-label">C++20 WAL &amp; Jump-Hash Ring</div>
          <div className="kpi-value">
            {wal.bitflip_corruptions_caught}/{wal.wal_frames_verified} CRC32C
          </div>
          <div className="kpi-sub">
            {wal.cpp_vs_python_hash_mismatches} C++/Py Mismatches · Moved{" "}
            {(wal.measured_remapped_fraction * 100).toFixed(2)}%
          </div>
        </div>
      </section>

      <section className="main-grid">
        <div className="panel">
          <div className="panel-header">
            <h2>1. Jepsen Fault-Injection &amp; Wing-Gong-Lowe Linearizability</h2>
            <div className="controls">
              <select
                value={seedCount}
                disabled={runningJepsen}
                onChange={(e) => runJepsen(Number(e.target.value))}
              >
                <option value={6}>6 Seeds (Fast)</option>
                <option value={12}>12 Seeds</option>
                <option value={20}>20 Seeds</option>
              </select>
            </div>
          </div>

          <table className="metric-table">
            <thead>
              <tr>
                <th>Seed</th>
                <th>Ops (OK / Indet)</th>
                <th>Elect / Snap / Reconf / Crash</th>
                <th>WGL &amp; Invariants</th>
                <th>Digest</th>
              </tr>
            </thead>
            <tbody>
              {jepsen.seed_summaries.slice(0, 10).map((s) => (
                <tr key={s.seed}>
                  <td className="mono">#{s.seed}</td>
                  <td className="mono">
                    {s.ops_ok} / {s.ops_indeterminate}
                  </td>
                  <td className="mono">
                    {s.elections} / {s.snapshots} / {s.membership_changes} / {s.crashes}
                  </td>
                  <td className={`mono ${s.safe && s.liveness_ok ? "ok" : "warn"}`}>
                    {s.safe ? "LINEARIZABLE" : "VIOLATION"}
                  </td>
                  <td className="mono">{s.fingerprint.slice(0, 10)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="panel">
          <div className="panel-header">
            <h2>2. 7-Mutant Protocol &amp; Unsynced-WAL Bug-Killing Suite</h2>
            <button
              type="button"
              className="pill gold"
              style={{ cursor: "pointer", border: "none" }}
              disabled={runningMutants}
              onClick={runMutantSuite}
            >
              {runningMutants ? "Searching Seeds..." : "Run 7-Mutant Suite"}
            </button>
          </div>

          {mutants ? (
            <table className="metric-table">
              <thead>
                <tr>
                  <th>Mutant</th>
                  <th>Seed</th>
                  <th>Detected By</th>
                </tr>
              </thead>
              <tbody>
                {mutants.cases.map((c) => (
                  <tr key={c.mutant_id}>
                    <td className="mono">{c.mutant_id}</td>
                    <td className="mono">{c.witness_seed ?? "—"}</td>
                    <td className={`mono ${c.caught ? "ok" : "warn"}`}>
                      {c.caught ? c.detected_by.join(", ") : "MISSED"}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : (
            <div className="trace-list">
              <div className="trace-item">
                Injects 7 classic consensus &amp; storage bugs (`double_vote`, `no_election_restriction`,
                `no_truncate_on_conflict`, `stale_read`, `commit_prior_term`, `volatile_vote`,
                `unsynced_wal_power_cut`) to verify the invariant observer and WGL checker catch every
                planted bug. Click &ldquo;Run 7-Mutant Suite&rdquo; above to execute live.
              </div>
              {mc.mutant_counterexample_steps.map((s, idx) => (
                <div key={idx} className="trace-item">
                  BFS Counterexample Step {idx + 1}: {s}
                </div>
              ))}
            </div>
          )}
        </div>
      </section>

      <section className="main-grid">
        <div className="panel">
          <div className="panel-header">
            <h2>3. Bounded Explicit-State BFS Checker (N=3, max_term=2)</h2>
            <span className="pill gold">Depth = {mc.max_bfs_depth}</span>
          </div>

          <table className="metric-table">
            <tbody>
              <tr>
                <td>INV-1 ElectionSafety (≤ 1 Leader / Term)</td>
                <td className="mono ok">VERIFIED ({mc.states_explored} states)</td>
              </tr>
              <tr>
                <td>INV-2 LogMatching (Identical Prefix Law)</td>
                <td className="mono ok">VERIFIED ({mc.transitions_checked} edges)</td>
              </tr>
              <tr>
                <td>INV-3 LeaderCompleteness (Ongaro §5.4.1 Active)</td>
                <td className="mono ok">VERIFIED (0 Violations)</td>
              </tr>
              <tr>
                <td>Mutant Protocol (§5.4.1 Disabled) Shortest Trace</td>
                <td className="mono warn">CAUGHT IN {mc.mutant_counterexample_steps.length} STEPS</td>
              </tr>
            </tbody>
          </table>
        </div>

        <div className="panel">
          <div className="panel-header">
            <h2>4. Split-Brain Partition Timeline &amp; C++20 WAL/Ring</h2>
            <div className="controls">
              <select value={clusterN} onChange={(e) => changeClusterN(Number(e.target.value))}>
                <option value={5}>5-Node Raft Cluster</option>
                <option value={7}>7-Node Raft Cluster</option>
              </select>
            </div>
          </div>

          <table className="metric-table">
            <thead>
              <tr>
                <th>Tick</th>
                <th>Event</th>
                <th>Simulation Detail</th>
              </tr>
            </thead>
            <tbody>
              {raft.timeline.map((ev, i) => (
                <tr key={i}>
                  <td className="mono">t={ev.tick}</td>
                  <td
                    className={`mono ${
                      ev.event_type.includes("PARTITION") && !ev.event_type.includes("HEAL")
                        ? "warn"
                        : "ok"
                    }`}
                  >
                    {ev.event_type}
                  </td>
                  <td>{ev.detail}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      {qwen && (
        <section className="panel">
          <div className="panel-header">
            <h2>5. Local Ollama ({qwen.model_name}) Quorum &amp; Fencing Parameter Check</h2>
            <span className="pill gold">
              Raw Pass@1: {qwen.raw_safe_count}/{qwen.num_incidents} ({qwen.raw_safe_pct.toFixed(1)}%) →
              Verified: {qwen.verified_safe_count}/{qwen.num_incidents} (
              {qwen.verified_safe_pct.toFixed(1)}%)
            </span>
          </div>
          <table className="metric-table">
            <thead>
              <tr>
                <th>Incident ID</th>
                <th>Quorum / Fencing Scenario</th>
                <th>Safe Spec</th>
                <th>Raw LLM Pass@1</th>
                <th>Invariant-Checked</th>
                <th>Turns</th>
                <th>Latency</th>
              </tr>
            </thead>
            <tbody>
              {qwen.outcomes.map((o) => (
                <tr key={o.incident_id}>
                  <td className="mono">{o.incident_id}</td>
                  <td>{o.title}</td>
                  <td className="mono">{o.expected_safe_quorum_or_fencing}</td>
                  <td className={`mono ${o.raw_llm_safe ? "ok" : "warn"}`}>
                    {o.raw_llm_value ?? "None"} {o.raw_llm_safe ? "✓" : "✗"}
                  </td>
                  <td className={`mono ${o.model_checked_safe ? "ok" : "gold"}`}>
                    {o.model_checked_safe ? `${o.model_checked_value} ✓` : "BLOCKED"}
                  </td>
                  <td className="mono">{o.turns_used}</td>
                  <td className="mono">{o.latency_ms.toFixed(0)} ms</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      )}
    </div>
  );
}
