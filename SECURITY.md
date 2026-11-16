# Security Policy

## Supported Versions

| Version | Supported |
| ------- | --------- |
| `1.0.x` | Yes |

## Threat Model & Formal Consensus Guarantees

`helios-consensus` enforces three independent safety boundaries for sovereign distributed state machines:

1. **Raft Election Restriction & Current-Term Commit Rule (Ongaro & Ousterhout §5.4.1–§5.4.2)**:
   - Voters reject `RequestVoteRPC` whenever `(cand_last_term, cand_last_index) < (voter_last_term, voter_last_index)`.
   - Leaders advance `commit_index` exclusively on log entries from `leader.current_term`, preventing Figure-8 indirect-commit overwrites across asymmetric network partitions.
2. **Explicit-State TLA+-Style BFS Invariant & Mutation Auditing**:
   - Explores reachable state graphs to verify `ElectionSafety`, `LogMatching`, and `LeaderCompleteness` and synthesizes minimal 4-step BFS counterexample traces when election-log restrictions are disabled.
3. **Chained HotStuff 3-Chain BFT Finality & WAL Bit-Rot Detection**:
   - Requires `2f + 1` distinct validator signatures per Quorum Certificate (`N = 3f + 1`), rejects equivocating same-view double-votes, and checksums every Write-Ahead Log frame via C++20 Castagnoli `CRC32C` (`0x82F63B78`).

## Reporting a Vulnerability

Please report security or consensus-safety vulnerabilities privately to **Nagarjuna Kamireddy** (`sendemailtonagarjuna@gmail.com`) rather than opening a public GitHub issue. Include a deterministic seed or message-interleaving trace if applicable.
