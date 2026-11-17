# ADR 0001: Logical-Tick Deterministic Simulation Testing (DST) for Raft

- **Status**: Accepted
- **Context**: Wall-clock `asyncio.sleep` consensus tests suffer from non-deterministic thread scheduling, hiding rare Figure-8 stale-leader and split-brain bugs.
- **Decision**: Drive `RaftClusterSimulator` and `SimCluster` exclusively via seeded logical ticks, deterministic staggered election timeouts, explicit directed-edge partition sets `(src, dst) in blocked`, and continuous per-tick `ElectionSafety`, `LogMatching`, `LeaderCompleteness`, and `StateMachineSafety` invariant assertions.
- **Consequences**: Any failing schedule (symmetric/asymmetric partitions, Jepsen bridge topologies, crashes, packet loss bursts, joint reconfigurations) can be replayed deterministically from a single integer seed in milliseconds.
