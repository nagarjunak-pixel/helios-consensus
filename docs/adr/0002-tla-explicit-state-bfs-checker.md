# ADR 0002: Bounded Explicit-State BFS Model Checking & Mutant Trace Synthesis

- **Status**: Accepted
- **Context**: Randomized simulation explores sampled schedules, whereas small-model exhaustive search can inspect every reachable interleaving within bounded bounds (`N=3, max_term=2, max_log=2`)—and must also demonstrate non-vacuity on a broken specification.
- **Decision**: Implement `explore_consensus_state_space` over canonical hashable `ClusterGlobalState` tuples (`nodes`, `committed_history`, `leaders_per_term`) across both the sound Raft transition relation (`enforce_up_to_date_vote=True`) and a mutated specification (`enforce_up_to_date_vote=False`).
- **Consequences**: Exhaustively checks `985` reachable states and `2,064` transitions (`depth=7`) with zero violations, while synthesizing a minimal **4-step BFS counterexample** when Ongaro §5.4.1 is disabled (`Scope note`: bounded explicit-state Python BFS, not an unbounded TLA+/TLC proof).
