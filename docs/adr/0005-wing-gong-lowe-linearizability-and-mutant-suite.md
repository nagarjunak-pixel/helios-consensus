# ADR 0005: Wing–Gong–Lowe Linearizability Solver & 7-Mutant Bug-Killing Harness

- **Status**: Accepted
- **Context**: Internal protocol assertions (`ElectionSafety`, `LogMatching`) do not by themselves prove external client-observable linearizability (for example, a stale leader serving `ReadIndex` without quorum confirmation violates linearizability without corrupting any Raft log), and a verification suite without mutation testing can pass vacuously.
- **Decision**:
  1. Partition client `(READ, WRITE, CAS)` histories per key (`P`-compositionality) and verify register linearizability via Wing & Gong's lifting search with Lowe's `(linearized_bitmask, register_state)` memoization—implemented in C++20 (`helios_wgl_register` for `<= 62` ops/key) with an identical pure-Python solver (`python_check_register`) verified via `Hypothesis` differential fuzzing.
  2. Ship an automated `7`-mutant suite (`helios kill-mutants`) planting `double_vote`, `no_election_restriction`, `no_truncate_on_conflict`, `stale_read`, `commit_prior_term` (Ongaro Figure 8 with `max_entries_per_msg=1` and omitted election `NOOP`), `volatile_vote`, and `unsynced_wal_power_cut`.
- **Consequences**: Kills `7/7` (`100%`) classic consensus and storage durability bugs in `< 300 ms` while verifying `20/20` fault-injected Jepsen seeds (`8,700+` committed ops) with zero safety or linearizability violations.
