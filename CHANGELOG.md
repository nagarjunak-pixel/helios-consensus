# Changelog

All notable changes to `helios-consensus` are documented in this file.

## [1.1.0] - 2026-11-18

### Added
- **Library-Style Raft Core (`helios_consensus.core`)**: Pure deterministic state machine (`RaftNode`, `RaftLog`, `Progress`) with `PreVote` (§9.6), two-window `CheckQuorum` + leader stickiness (§6), `Probe`/`Replicate`/`Snapshot` flow control, conflict-term log backtracking, `ReadIndex` linearizable reads (§8), `TimeoutNow` leadership transfer (§3.10), single-server `ADD_LEARNER`/`PROMOTE`/`REMOVE`, and Ongaro §6 two-phase `Joint Consensus` (`C_{old,new}` -> `C_new`).
- **Crash-Safe Binary Storage & Chunked Snapshots**: `SegmentedWAL` with `25`-byte Castagnoli `CRC32C` binary frames, segment rotation, snapshot garbage collection, `PosixDiskFS` (`os.fsync` + atomic directory sync), `SimulatedDiskFS` (volatile page cache vs durable platters, torn power-cut tails, silent bitrot injection), and `SnapshotAssembler` (`CRC32C` per chunk + RFC-6962 SHA-256 Merkle root).
- **520-Line Native C++20 Acceleration Engine (`_csrc/helios_kernels.cpp`)**: Slicing-by-8 `CRC32C`, Lamping–Veach Jump Consistent Hash, binary WAL encoder/recovery scanner, Wing–Gong–Lowe bitmask register linearizability solver (`N <= 62`), single/joint quorum watermark, and FIPS 180-4 SHA-256 + RFC-6962 Merkle root — packaged inside the wheel with bit-exact Python fallbacks.
- **Asyncio TCP Transport & Multi-Raft Sharding**: `RaftTCPServer` and `RaftKVClient` (CRC32C-framed loopback/LAN RPCs, leader hints, `(client_id, seq)` session deduplication, chunked `InstallSnapshot` catch-up) plus `MultiRaftCluster` (Jump-Hash shard placement, online `split_range` with session table cloning and epoch fencing, and `TimeoutNow` leader rebalancing).
- **Jepsen-Style Simulator, WGL Linearizability & 7-Mutant Suite**: Seeded fault-injection harness (`SimCluster`), P-compositional Wing–Gong–Lowe `(READ, WRITE, CAS)` linearizability checker, and `helios kill-mutants` verifying `7/7` (`100%`) protocol & unsynced-WAL bug kills (`double_vote`, `no_election_restriction`, `no_truncate_on_conflict`, `stale_read`, `commit_prior_term`, `volatile_vote`, `unsynced_wal_power_cut`).

### Changed
- Moved C++20 kernel source into `src/helios_consensus/_csrc/helios_kernels.cpp` and added `py.typed`, classifiers, sdist includes, and accurate `Apache-2.0` SPDX metadata for PyPI distribution.
- Expanded `pytest` + `Hypothesis` test suite to 59 test cases (`97.39%` line coverage across `4,150+` Python statements).

## [1.0.0] - 2026-11-16

### Added
- Initial logical-tick 5-node Raft split-brain simulator, bounded 3-node explicit-state BFS checker (`985` states), HotStuff 3-chain BFT simulation, C++20 Jump-Hash/CRC32C helper, FastAPI server, and React Workbench UI.
