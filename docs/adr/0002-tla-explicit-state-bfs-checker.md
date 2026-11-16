# ADR 0002: Explicit-State TLA+-Style BFS Model Checking & Mutant Trace Synthesis

- **Status**: Accepted
- **Context**: Simulation tests finite schedules, whereas safety proofs require exhaustive exploration over non-deterministic election and replication interleavings—plus proof that the checker is non-vacuous.
- **Decision**: Implement `explore_consensus_state_space` over canonical hashable `ClusterGlobalState` tuples (`nodes`, `committed_history`, `leaders_per_term`) across BOTH the sound Raft specification (`enforce_up_to_date_vote=True`) and a mutated specification (`enforce_up_to_date_vote=False`).
- **Consequences**: Verifies all `985` reachable states / `2,064` transitions (`depth=7`) for sound Raft while synthesizing the minimal **4-step BFS counterexample** demonstrating stale-leader overwrite when Ongaro §5.4.1 is disabled.
