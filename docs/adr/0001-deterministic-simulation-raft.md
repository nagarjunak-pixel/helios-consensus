# ADR 0001: FoundationDB-Style Deterministic Simulation Testing (DST) for Raft

- **Status**: Accepted
- **Context**: Wall-clock `asyncio.sleep` consensus tests suffer from non-deterministic thread scheduling, hiding rare Figure-8 stale-leader and split-brain bugs.
- **Decision**: Drive `RaftClusterSimulator` exclusively via logical ticks, deterministic staggered election timeouts, explicit directed-edge partition sets `(src, dst) in blocked_links`, and continuous per-tick `ElectionSafety` + `LogMatching` invariant assertions.
- **Consequences**: Identical 54-tick asymmetric `{n0, n1} | {n2, n3, n4}` partition schedules reproduce the exact state-machine SHA-256 digest `56f7268125480081` on every OS and CPU architecture in `<5 ms`.
