"""Sharded Multi-Raft engine with Jump-Hash routing, online range split & leader rebalancing.

Models how CockroachDB and TiKV scale horizontal state across physical stores:

* Keys map deterministically to lexicographic ``KeyRange(start_key, end_key)`` descriptors
  (with initial shard placement computed via our C++20 Jump Consistent Hash kernel).
* Each physical ``Store`` multiplexes multiple independent ``RaftNode`` replicas over one
  tick loop and message fabric, tagging envelopes with ``range_id`` and ``range_epoch``.
* ``split_range(range_id, split_key)`` atomically partitions a range's keyspace and clones
  active client session deduplication state so retried in-flight writes remain linearizable
  across a split boundary.
* ``rebalance_leaders()`` inspects leader counts per physical store and uses Raft's
  ``transfer_leadership`` (`TimeoutNow`) to equalize range leadership (`max - min <= 1`).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

from helios_consensus.core.messages import Membership, Message, Snapshot
from helios_consensus.core.node import NotLeaderError, RaftConfig, RaftNode, StateRole
from helios_consensus.core.storage import MemoryStorage
from helios_consensus.kernels.native_ring import get_helios_bridge
from helios_consensus.kv.state_machine import KVCommand, KVOp, KVStateMachine


@dataclass(frozen=True, slots=True)
class KeyRange:
    range_id: int
    start_key: str  # inclusive; "" means -infinity
    end_key: str  # exclusive; "" means +infinity
    epoch: int = 1
    voters: frozenset[int] = frozenset()

    def contains(self, key: str) -> bool:
        if self.start_key and key < self.start_key:
            return False
        return not (self.end_key and key >= self.end_key)


@dataclass(slots=True)
class RangeReplica:
    desc: KeyRange
    storage: MemoryStorage
    raft: RaftNode
    sm: KVStateMachine = field(default_factory=KVStateMachine)


@dataclass(frozen=True, slots=True)
class MultiRaftEnvelope:
    range_id: int
    epoch: int
    message: Message


class MultiRaftCluster:
    """Deterministic multi-store, multi-range Raft cluster with splits & leader balancing."""

    def __init__(
        self,
        *,
        num_stores: int = 5,
        initial_boundaries: list[str] | None = None,
        replicas_per_range: int = 3,
        seed: int = 1,
        raft_config: RaftConfig | None = None,
    ) -> None:
        if replicas_per_range > num_stores or replicas_per_range < 1:
            raise ValueError("1 <= replicas_per_range <= num_stores required")
        self.num_stores = num_stores
        self.replicas_per_range = replicas_per_range
        self.seed = seed
        self.cfg = raft_config or RaftConfig(election_tick=10, heartbeat_tick=2)

        self.stores: dict[int, dict[int, RangeReplica]] = {
            sid: {} for sid in range(1, num_stores + 1)
        }
        self.ranges: dict[int, KeyRange] = {}
        self._in_flight: list[MultiRaftEnvelope] = []
        self._client_seq = 0
        self._read_req_id = 0
        self._pending_read_values: dict[tuple[int, int], tuple[str, int | None]] = {}
        self.next_range_id = 1

        cuts = sorted(set(initial_boundaries or ["g", "n", "t"]))
        starts = ["", *cuts]
        ends = [*cuts, ""]
        for s_key, e_key in zip(starts, ends, strict=True):
            rid = self.next_range_id
            self.next_range_id += 1
            voters = self._select_voters_jump_hash(rid)
            self._install_range(
                KeyRange(range_id=rid, start_key=s_key, end_key=e_key, epoch=1, voters=voters)
            )

    def _select_voters_jump_hash(self, range_id: int) -> frozenset[int]:
        bridge = get_helios_bridge()
        chosen: list[int] = []
        salt = 0
        while len(chosen) < self.replicas_per_range:
            raw_key = int.from_bytes(
                hashlib.blake2b(f"range:{range_id}:{salt}".encode(), digest_size=8).digest(),
                "little",
            )
            bucket = bridge.jump_hash(raw_key, self.num_stores) + 1
            if bucket not in chosen:
                chosen.append(bucket)
            salt += 1
        return frozenset(chosen)

    def _install_range(
        self,
        desc: KeyRange,
        *,
        initial_data: dict[str, int] | None = None,
        initial_sessions: dict[int, tuple[int, bool]] | None = None,
    ) -> None:
        self.ranges[desc.range_id] = desc
        sm_template = KVStateMachine()
        if initial_data:
            sm_template.data = dict(initial_data)
        if initial_sessions:
            sm_template.sessions = dict(initial_sessions)
        snap = Snapshot(
            index=0,
            term=0,
            membership=Membership(voters=desc.voters),
            data=sm_template.snapshot() if (initial_data or initial_sessions) else "",
        )
        for sid in sorted(desc.voters):
            store = MemoryStorage(snap)
            raft = RaftNode(
                sid,
                self.cfg,
                seed=self.seed * 1_009 + desc.range_id * 37,
                snapshot=snap,
            )
            self.stores[sid][desc.range_id] = RangeReplica(
                desc=desc,
                storage=store,
                raft=raft,
                sm=KVStateMachine.restore(snap.data),
            )

    # ----------------------------------------------------------------- stepping & routing
    def locate_range(self, key: str) -> KeyRange:
        for desc in sorted(self.ranges.values(), key=lambda r: r.start_key):
            if desc.contains(key):
                return desc
        raise KeyError(f"no range covers key {key!r}")

    def range_leader(self, range_id: int) -> tuple[int, RangeReplica] | None:
        desc = self.ranges[range_id]
        best: tuple[int, RangeReplica] | None = None
        for sid in sorted(desc.voters):
            rep = self.stores[sid].get(range_id)
            if (
                rep is not None
                and rep.raft.role is StateRole.LEADER
                and (best is None or rep.raft.term > best[1].raft.term)
            ):
                best = (sid, rep)
        return best

    def step_ticks(self, ticks: int = 1) -> None:
        for _ in range(ticks):
            for sid in sorted(self.stores):
                for rep in self.stores[sid].values():
                    rep.raft.tick()
            self.pump()

    def pump(self, max_rounds: int = 64) -> None:
        for _ in range(max_rounds):
            moved = False
            for sid in sorted(self.stores):
                for rid, rep in list(self.stores[sid].items()):
                    if rep.raft.has_ready():
                        moved = True
                        rd = rep.raft.ready()
                        rep.storage.save(rd)
                        if rd.snapshot is not None:
                            rep.sm = KVStateMachine.restore(rd.snapshot.data)
                        for entry in rd.committed_entries:
                            rep.sm.apply(entry)
                        for rs in rd.read_states:
                            wait_key = (rid, rs.request_id)
                            if wait_key in self._pending_read_values:
                                k_name, _ = self._pending_read_values[wait_key]
                                self._pending_read_values[wait_key] = (
                                    k_name,
                                    rep.sm.read(k_name),
                                )
                        rep.raft.advance(rd)
                        for msg in rd.messages:
                            self._in_flight.append(
                                MultiRaftEnvelope(range_id=rid, epoch=rep.desc.epoch, message=msg)
                            )
            if self._in_flight:
                batch, self._in_flight = self._in_flight, []
                for env in batch:
                    target_rep = self.stores.get(env.message.dst, {}).get(env.range_id)
                    if target_rep is not None and target_rep.desc.epoch == env.epoch:
                        target_rep.raft.step(env.message)
                        moved = True
            if not moved:
                break

    def wait_all_leaders(self, max_ticks: int = 80) -> None:
        for _ in range(max_ticks):
            if all(self.range_leader(rid) is not None for rid in self.ranges):
                self.step_ticks(3)
                return
            self.step_ticks(1)
        missing = [rid for rid in self.ranges if self.range_leader(rid) is None]
        raise RuntimeError(f"ranges without leader after {max_ticks} ticks: {missing}")

    # ----------------------------------------------------------------- KV operations
    def put(self, key: str, value: int, *, client_id: int = 1, seq: int | None = None) -> int:
        desc = self.locate_range(key)
        ldr = self.range_leader(desc.range_id)
        if ldr is None:
            raise NotLeaderError(None)
        if seq is None:
            self._client_seq += 1
            seq = self._client_seq
        cmd = KVCommand(op=KVOp.PUT, key=key, value=value, client=client_id, seq=seq)
        idx = ldr[1].raft.propose(cmd.encode())
        self.pump()
        return idx

    def get_linearizable(self, key: str) -> int | None:
        desc = self.locate_range(key)
        ldr = self.range_leader(desc.range_id)
        if ldr is None:
            raise NotLeaderError(None)
        self._read_req_id += 1
        req_id = self._read_req_id
        wait_key = (desc.range_id, req_id)
        self._pending_read_values[wait_key] = (key, None)
        ldr[1].raft.read_index(req_id)
        self.pump()
        if wait_key in self._pending_read_values:
            _, val = self._pending_read_values.pop(wait_key)
            return val
        return ldr[1].sm.read(key)

    # ----------------------------------------------------------------- range split & rebalance
    def split_range(self, range_id: int, split_key: str) -> tuple[KeyRange, KeyRange]:
        """Atomically split ``range_id`` at ``split_key`` into left & right ranges."""
        desc = self.ranges[range_id]
        if not desc.contains(split_key) or split_key == desc.start_key:
            raise ValueError(
                f"split_key {split_key!r} must lie strictly inside [{desc.start_key!r}, {desc.end_key!r})"
            )
        ldr = self.range_leader(range_id)
        if ldr is None:
            raise RuntimeError(f"range {range_id} has no active leader to coordinate split")
        self.pump()
        leader_sm = ldr[1].sm

        left_data = {k: v for k, v in leader_sm.data.items() if k < split_key}
        right_data = {k: v for k, v in leader_sm.data.items() if k >= split_key}
        sessions_copy = dict(leader_sm.sessions)

        new_rid = self.next_range_id
        self.next_range_id += 1
        left_desc = KeyRange(
            range_id=desc.range_id,
            start_key=desc.start_key,
            end_key=split_key,
            epoch=desc.epoch + 1,
            voters=desc.voters,
        )
        right_voters = self._select_voters_jump_hash(new_rid)
        right_desc = KeyRange(
            range_id=new_rid,
            start_key=split_key,
            end_key=desc.end_key,
            epoch=1,
            voters=right_voters,
        )

        # Replace left replicas under incremented epoch (fences stale pre-split messages)
        # and spawn right replicas with cloned session table.
        for sid in desc.voters:
            self.stores[sid].pop(desc.range_id, None)
        self._install_range(left_desc, initial_data=left_data, initial_sessions=sessions_copy)
        self._install_range(right_desc, initial_data=right_data, initial_sessions=sessions_copy)
        self.wait_all_leaders()
        return left_desc, right_desc

    def leader_counts_by_store(self) -> dict[int, int]:
        counts = {sid: 0 for sid in self.stores}
        for rid in self.ranges:
            ldr = self.range_leader(rid)
            if ldr is not None:
                counts[ldr[0]] += 1
        return counts

    def rebalance_leaders(self, max_passes: int = 16) -> dict[int, int]:
        """Transfer range leadership via ``TimeoutNow`` to minimize store leader skew."""
        for _ in range(max_passes):
            counts = self.leader_counts_by_store()
            donors = sorted(counts, key=lambda s: (-counts[s], s))
            moved = False
            for donor in donors:
                for rid in sorted(self.ranges):
                    ldr = self.range_leader(rid)
                    if ldr is None or ldr[0] != donor:
                        continue
                    voters = sorted(self.ranges[rid].voters - {donor})
                    if not voters:
                        continue
                    best_target = min(voters, key=lambda v: (counts[v], v))
                    if counts[donor] - counts[best_target] >= 2:
                        ldr[1].raft.transfer_leadership(best_target)
                        self.pump()
                        self.step_ticks(3)
                        moved = True
                        break
                if moved:
                    break
            if not moved:
                break
        return self.leader_counts_by_store()
