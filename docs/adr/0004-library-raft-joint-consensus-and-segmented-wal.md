# ADR 0004: Library-Style Raft Separation, Joint Consensus & Segmented Binary WAL

- **Status**: Accepted
- **Context**: Coupling Raft state transitions directly to sockets and filesystem calls makes deterministic fault injection awkward and prevents sharing one consensus core across in-memory DST, Multi-Raft shards, and real `asyncio` TCP servers.
- **Decision**:
  1. Model `RaftNode` after `etcd/raft`'s pure `step(Message)` / `tick()` -> `Ready` -> `advance(Ready)` contract with zero direct I/O.
  2. Support both single-server learner staging (`ADD_LEARNER` -> `PROMOTE` / `REMOVE`) and Ongaro §6 two-phase **Joint Consensus** (`ENTER_JOINT` `C_{old,new}` -> automatic leader-proposed `LEAVE_JOINT` `C_new`) so arbitrary disjoint voter migrations (`{1,2,3} -> {4,5,6}`) preserve dual-quorum intersection.
  3. Implement `SegmentedWAL` using `25`-byte Castagnoli `CRC32C` binary frames over `DiskFS` (`PosixDiskFS` for real `os.fsync` and `SimulatedDiskFS` for volatile page-cache vs durable platter power-cut testing), plus `SnapshotAssembler` verified by per-chunk `CRC32C` and an RFC-6962 SHA-256 Merkle root.
- **Consequences**: The exact same `RaftNode` drives `SimCluster`, `MultiRaftCluster`, and `RaftTCPServer`. Torn tail writes from mid-append power loss are truncated cleanly at recovery, while silent bitrot inside sealed segments or snapshots raises `WalCorruptionError` and un-fsynced WAL configurations (`sync_wal=False`) are deterministically caught under crash injection.
