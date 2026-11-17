"""Jepsen-style deterministic cluster simulation.

A single seeded PRNG drives every source of nondeterminism (message delays, loss,
duplication, client operations, fault schedule, election timeouts), so any failing run can
be replayed exactly from its seed.

Each run:

1. boots an N-node Raft cluster running the KV state machine;
2. lets concurrent clients issue reads, writes and compare-and-set operations, recording a
   history with invocation/completion times (timeouts are recorded as indeterminate);
3. injects faults (partitions, bridges, crashes with restart from stable storage, process
   pauses, packet-loss bursts, leadership transfers, membership changes);
4. checks Raft's safety invariants continuously and linearizability of the history at the
   end, then heals everything and verifies the cluster makes progress again.
"""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass, field
from enum import StrEnum

from helios_consensus.check.invariants import InvariantObserver, Violation
from helios_consensus.check.linearizability import (
    LinearizabilityReport,
    Operation,
    OpKind,
    check_history,
)
from helios_consensus.core.log import LogSafetyError
from helios_consensus.core.messages import ConfChange, ConfChangeType, Membership, Snapshot
from helios_consensus.core.node import (
    NotLeaderError,
    ProposalDroppedError,
    RaftConfig,
    RaftNode,
    StateRole,
)
from helios_consensus.core.segmented_wal import SegmentedWAL, SimulatedDiskFS, WalCorruptionError
from helios_consensus.core.storage import MemoryStorage, Storage
from helios_consensus.kv.state_machine import KVCommand, KVOp, KVStateMachine
from helios_consensus.sim.network import NetworkStats, SimNetwork


class Fault(StrEnum):
    PARTITION = "partition"
    ISOLATE_LEADER = "isolate_leader"
    BRIDGE = "bridge"
    CRASH = "crash"
    PAUSE = "pause"
    LOSS_BURST = "loss_burst"
    TRANSFER = "transfer_leader"
    MEMBERSHIP = "membership"
    JOINT_RECONFIG = "joint_reconfig"
    HEAL = "heal"


ALL_FAULTS: tuple[Fault, ...] = tuple(Fault)


@dataclass(frozen=True, slots=True)
class SimConfig:
    seed: int = 1
    nodes: int = 5
    ticks: int = 1500
    settle_ticks: int = 300
    clients: int = 4
    keys: int = 3
    client_rate: float = 0.35
    op_timeout: int = 80
    min_delay: int = 1
    max_delay: int = 4
    drop_rate: float = 0.01
    dup_rate: float = 0.01
    nemesis_interval: int = 70
    faults: tuple[Fault, ...] = ALL_FAULTS
    snapshot_threshold: int = 32
    log_matching_every: int = 25
    use_segmented_wal: bool = False
    sync_wal: bool = True
    raft: RaftConfig = field(default_factory=lambda: RaftConfig(election_tick=20, heartbeat_tick=3))

    def with_bugs(self, *bugs: str) -> SimConfig:
        r = self.raft
        raft = RaftConfig(
            election_tick=r.election_tick,
            heartbeat_tick=r.heartbeat_tick,
            max_entries_per_msg=r.max_entries_per_msg,
            max_inflight=r.max_inflight,
            pre_vote=r.pre_vote,
            check_quorum=r.check_quorum,
            bugs=frozenset(bugs),
        )
        return SimConfig(
            **{**{f: getattr(self, f) for f in self.__dataclass_fields__}, "raft": raft}
        )


@dataclass(slots=True)
class SimNode:
    node_id: int
    storage: Storage
    disk: SimulatedDiskFS | None = None
    raft: RaftNode | None = None
    sm: KVStateMachine = field(default_factory=KVStateMachine)
    incarnation: int = 0
    paused_until: int = 0
    restart_at: int = 0
    decommissioned: bool = False

    @property
    def alive(self) -> bool:
        return self.raft is not None


@dataclass(slots=True)
class _Pending:
    op_index: int
    node_id: int
    incarnation: int
    deadline: int
    kind: OpKind
    key: str
    request_id: int = 0
    read_index: int = -1


@dataclass(slots=True)
class _Client:
    client_id: int
    leader_guess: int
    seq: int = 0
    pending: _Pending | None = None


@dataclass(slots=True)
class _OpRecord:
    process: int
    kind: OpKind
    key: str
    value: int | None
    call: int
    expected: int | None = None
    ret: int | None = None
    ok: bool | None = None
    status: str = "pending"  # ok | fail | info


@dataclass(slots=True)
class SimReport:
    seed: int
    bugs: list[str]
    ticks: int
    violations: list[Violation]
    linearizability: LinearizabilityReport
    ops_ok: int
    ops_failed: int
    ops_indeterminate: int
    faults_injected: dict[str, int]
    elections: int
    max_term: int
    snapshots_taken: int
    snapshots_installed: int
    membership_changes: int
    crashes: int
    restarts: int
    network: NetworkStats
    invariant_checks: int
    liveness_ok: bool
    final_voters: list[int]
    fingerprint: str
    event_log: list[str]

    @property
    def safe(self) -> bool:
        return not self.violations and self.linearizability.ok

    @property
    def detected_by(self) -> list[str]:
        found = sorted({v.invariant for v in self.violations})
        if not self.linearizability.ok:
            found.append("Linearizability")
        return found


class SimCluster:
    def __init__(self, cfg: SimConfig) -> None:
        self.cfg = cfg
        self.rng = random.Random(cfg.seed)
        self.now = 0
        self.net = SimNetwork(
            rng=random.Random(cfg.seed ^ 0x5EED),
            min_delay=cfg.min_delay,
            max_delay=cfg.max_delay,
            drop_rate=cfg.drop_rate,
            dup_rate=cfg.dup_rate,
        )
        self.base_drop_rate = cfg.drop_rate
        voters = frozenset(range(1, cfg.nodes + 1))
        self.bootstrap = Snapshot(index=0, term=0, membership=Membership(voters=voters))
        self.observer = InvariantObserver()
        self.nodes: dict[int, SimNode] = {}
        for nid in sorted(voters):
            self._create_node(nid)
        self.next_node_id = cfg.nodes + 1
        self.clients = [
            _Client(client_id=c, leader_guess=self.rng.choice(sorted(voters)))
            for c in range(1, cfg.clients + 1)
        ]
        self.history: list[_OpRecord] = []
        self.write_counter = 0
        self.request_counter = 0
        self.loss_burst_until = 0
        self.faults_injected: dict[str, int] = {}
        self.elections = 0
        self.snapshots_taken = 0
        self.snapshots_installed = 0
        self.membership_changes = 0
        self.crashes = 0
        self.restarts = 0
        self.event_log: list[str] = []
        self._pending_member: int | None = None
        self._removed_pending: set[int] = set()

    # ======================================================================= lifecycle
    def _create_node(self, nid: int) -> SimNode:
        if self.cfg.use_segmented_wal:
            disk = SimulatedDiskFS(seed=self.cfg.seed * 97 + nid)
            storage: Storage = SegmentedWAL(
                disk, self.bootstrap, max_segment_bytes=4096, sync=self.cfg.sync_wal
            )
            node = SimNode(node_id=nid, storage=storage, disk=disk)
        else:
            node = SimNode(node_id=nid, storage=MemoryStorage(self.bootstrap))
        self.nodes[nid] = node
        self._boot(node)
        return node

    def _boot(self, node: SimNode) -> None:
        if node.disk is not None:
            try:
                node.storage = SegmentedWAL(
                    node.disk, self.bootstrap, max_segment_bytes=4096, sync=self.cfg.sync_wal
                )
            except WalCorruptionError as exc:
                self.observer.violations.append(
                    Violation(
                        invariant="WalDurability",
                        tick=self.now,
                        detail=f"n{node.node_id}: {exc}",
                    )
                )
                return
        hs, snap, entries = node.storage.load()
        node.raft = RaftNode(
            node.node_id,
            self.cfg.raft,
            seed=self.cfg.seed * 31 + node.incarnation,
            snapshot=snap,
            hard_state=hs,
            entries=entries,
        )
        node.sm = KVStateMachine.restore(snap.data)
        self.observer.on_snapshot_applied(node.node_id, node.incarnation, snap.index)

    def crash(self, nid: int, restart_after: int | None) -> None:
        node = self.nodes[nid]
        if not node.alive:
            return
        node.raft = None
        if node.disk is not None:
            node.disk.power_cut(tear_unsynced_tail=self.cfg.sync_wal)
        node.incarnation += 1
        node.restart_at = self.now + restart_after if restart_after is not None else 0
        self.crashes += 1
        self._log(f"crash n{nid}" + (f" (restart in {restart_after})" if restart_after else ""))

    def restart(self, nid: int) -> None:
        node = self.nodes[nid]
        if node.alive or node.decommissioned:
            return
        node.restart_at = 0
        self._boot(node)
        self.restarts += 1
        self._log(f"restart n{nid} from stable storage")

    def live_nodes(self) -> list[SimNode]:
        return [n for n in self.nodes.values() if n.raft is not None]

    def leader(self) -> SimNode | None:
        best: SimNode | None = None
        for n in self.live_nodes():
            r = n.raft
            assert r is not None
            if r.role is StateRole.LEADER and (
                best is None or best.raft is None or r.term > best.raft.term
            ):
                best = n
        return best

    def _log(self, text: str) -> None:
        self.event_log.append(f"t={self.now}: {text}")

    # ======================================================================= main loop
    def run(self) -> SimReport:
        total = self.cfg.ticks + self.cfg.settle_ticks
        while self.now < total:
            self.now += 1
            settling = self.now > self.cfg.ticks
            if self.now == self.cfg.ticks + 1:
                self._heal_everything()
            self._deliver()
            self._tick_nodes()
            self._process_all_ready()
            self._clients_act(issue=not settling or self.now < total - self.cfg.op_timeout)
            self._expire_ops()
            if not settling and self.now % self.cfg.nemesis_interval == 0:
                self._nemesis()
            self._timed_recoveries()
            self.observer.check_roles([n.raft for n in self.live_nodes() if n.raft], self.now)
            if self.now % self.cfg.log_matching_every == 0:
                self.observer.check_log_matching(
                    [n.raft for n in self.live_nodes() if n.raft], self.now
                )
            if self.observer.violations and len(self.observer.violations) > 20:
                break
        for c in self.clients:
            if c.pending is not None:
                self._finish(c, status="info")
        return self._report()

    def _deliver(self) -> None:
        for m in self.net.due(self.now):
            node = self.nodes.get(m.dst)
            if node is None or node.raft is None:
                self.net.stats.dropped_dead += 1
                continue
            if node.paused_until > self.now:
                self.net.defer(m, node.paused_until)
                continue
            self.net.stats.delivered += 1
            self._guard(node, lambda r, msg=m: r.step(msg))

    def _tick_nodes(self) -> None:
        for node in self.live_nodes():
            if node.paused_until > self.now:
                continue
            self._guard(node, lambda r: r.tick())

    def _guard(self, node: SimNode, action: object) -> None:
        r = node.raft
        assert r is not None
        was_leader = r.role is StateRole.LEADER
        try:
            action(r)  # type: ignore[operator]
        except LogSafetyError as exc:
            self.observer.violations.append(
                Violation(invariant="LogSafety", tick=self.now, detail=f"n{node.node_id}: {exc}")
            )
        if r.role is StateRole.LEADER and not was_leader:
            self.elections += 1
            self.observer.on_leader(r, self.now)

    def _process_all_ready(self) -> None:
        for node in list(self.live_nodes()):
            if node.paused_until > self.now:
                continue
            self._process_ready(node)

    def _process_ready(self, node: SimNode) -> None:
        r = node.raft
        assert r is not None
        if not r.has_ready():
            return
        rd = r.ready()
        node.storage.save(rd)  # 1. persist before anything leaves the node
        for m in rd.messages:  # 2. send
            self.net.send(m, self.now)
        if rd.snapshot is not None:  # 3. apply
            node.sm = KVStateMachine.restore(rd.snapshot.data)
            self.snapshots_installed += 1
            self.observer.on_snapshot_applied(node.node_id, node.incarnation, rd.snapshot.index)
        for e in rd.committed_entries:
            self.observer.on_apply(node.node_id, node.incarnation, e, self.now)
            result = node.sm.apply(e)
            if result is not None:
                self._complete_write(node, result.client, result.seq, result.result)
        for rs in rd.read_states:
            self._read_ready(node, rs.request_id, rs.index)
        r.advance(rd)
        self._serve_reads(node)
        self._maybe_compact(node)

    def _maybe_compact(self, node: SimNode) -> None:
        r = node.raft
        assert r is not None
        applied = r.log.applied
        if applied - r.log.snapshot.index >= self.cfg.snapshot_threshold:
            snap = r.compact(applied, node.sm.snapshot())
            node.storage.save_snapshot(snap)
            self.snapshots_taken += 1

    # ======================================================================= clients
    def _clients_act(self, *, issue: bool) -> None:
        for c in self.clients:
            if c.pending is None and issue and self.rng.random() < self.cfg.client_rate:
                self._issue(c)

    def _issue(self, c: _Client) -> None:
        key = f"k{self.rng.randrange(self.cfg.keys)}"
        roll = self.rng.random()
        kind = OpKind.READ if roll < 0.4 else OpKind.WRITE if roll < 0.8 else OpKind.CAS
        c.seq += 1
        value: int | None = None
        expected: int | None = None
        if kind is not OpKind.READ:
            self.write_counter += 1
            value = self.write_counter
        if kind is OpKind.CAS:
            recent = [
                h.value
                for h in self.history[-30:]
                if h.key == key and h.value is not None and h.kind is not OpKind.READ
            ]
            expected = self.rng.choice(recent) if recent and self.rng.random() < 0.8 else None
        record = _OpRecord(
            process=c.client_id, kind=kind, key=key, value=value, call=self.now, expected=expected
        )
        self.history.append(record)
        op_index = len(self.history) - 1

        target = self.nodes.get(c.leader_guess)
        if target is None or target.raft is None or target.paused_until > self.now:
            self._fail_now(c, record)
            return
        r = target.raft
        try:
            if kind is OpKind.READ:
                self.request_counter += 1
                r.read_index(self.request_counter)
                c.pending = _Pending(
                    op_index=op_index,
                    node_id=target.node_id,
                    incarnation=target.incarnation,
                    deadline=self.now + self.cfg.op_timeout,
                    kind=kind,
                    key=key,
                    request_id=self.request_counter,
                )
            else:
                assert value is not None
                cmd = KVCommand(
                    op=KVOp.PUT if kind is OpKind.WRITE else KVOp.CAS,
                    key=key,
                    value=value,
                    expected=expected,
                    client=c.client_id,
                    seq=c.seq,
                )
                r.propose(cmd.encode())
                c.pending = _Pending(
                    op_index=op_index,
                    node_id=target.node_id,
                    incarnation=target.incarnation,
                    deadline=self.now + self.cfg.op_timeout,
                    kind=kind,
                    key=key,
                )
        except NotLeaderError as exc:
            self._fail_now(c, record)
            hint = exc.leader_hint
            c.leader_guess = hint if hint is not None else self.rng.choice(sorted(self.nodes))
        except ProposalDroppedError:
            self._fail_now(c, record)

    def _fail_now(self, c: _Client, record: _OpRecord) -> None:
        record.status = "fail"
        record.ret = self.now
        c.leader_guess = self.rng.choice(sorted(self.nodes))

    def _complete_write(self, node: SimNode, client: int, seq: int, result: bool) -> None:
        c = self.clients[client - 1]
        p = c.pending
        if p is None or p.kind is OpKind.READ or c.seq != seq:
            return
        if p.node_id != node.node_id or p.incarnation != node.incarnation:
            return
        rec = self.history[p.op_index]
        rec.ok = result if rec.kind is OpKind.CAS else True
        self._finish(c, status="ok")

    def _read_ready(self, node: SimNode, request_id: int, index: int) -> None:
        for c in self.clients:
            p = c.pending
            if (
                p is not None
                and p.kind is OpKind.READ
                and p.request_id == request_id
                and p.node_id == node.node_id
                and p.incarnation == node.incarnation
            ):
                p.read_index = index

    def _serve_reads(self, node: SimNode) -> None:
        for c in self.clients:
            p = c.pending
            if (
                p is not None
                and p.kind is OpKind.READ
                and p.read_index >= 0
                and p.node_id == node.node_id
                and p.incarnation == node.incarnation
                and node.sm.applied_index >= p.read_index
            ):
                self.history[p.op_index].value = node.sm.read(p.key)
                self._finish(c, status="ok")

    def _expire_ops(self) -> None:
        for c in self.clients:
            p = c.pending
            if p is not None and self.now >= p.deadline:
                # Reads have no side effects: a timed-out read is simply dropped. Writes may
                # still commit later, so they become indeterminate.
                self._finish(c, status="fail" if p.kind is OpKind.READ else "info")
                c.leader_guess = self.rng.choice(sorted(self.nodes))

    def _finish(self, c: _Client, *, status: str) -> None:
        p = c.pending
        assert p is not None
        rec = self.history[p.op_index]
        rec.status = status
        rec.ret = self.now if status != "info" else None
        c.pending = None

    # ======================================================================= nemesis
    def _nemesis(self) -> None:
        fault = self.rng.choice(self.cfg.faults)
        self.faults_injected[fault.value] = self.faults_injected.get(fault.value, 0) + 1
        members = self._current_members()
        if fault is Fault.HEAL:
            self.net.heal()
            self._log("heal network")
        elif fault is Fault.PARTITION:
            ids = sorted(members)
            self.rng.shuffle(ids)
            cut = self.rng.randint(1, len(ids) // 2)
            self.net.isolate(set(ids[:cut]), set(ids[cut:]))
            self._log(f"partition {sorted(ids[:cut])} | {sorted(ids[cut:])}")
        elif fault is Fault.ISOLATE_LEADER:
            ldr = self.leader()
            if ldr is not None:
                self.net.isolate({ldr.node_id}, set(members) - {ldr.node_id})
                self._log(f"isolate leader n{ldr.node_id}")
        elif fault is Fault.BRIDGE:
            ids = sorted(members)
            if len(ids) >= 5:
                self.rng.shuffle(ids)
                mid = len(ids) // 2
                bridge_node = ids[mid]
                self.net.bridge(set(ids[:mid]), set(ids[mid + 1 :]), bridge_node)
                self.net.blocked |= set()
                self._log(f"bridge {sorted(ids[:mid])} <-n{bridge_node}-> {sorted(ids[mid + 1 :])}")
        elif fault is Fault.CRASH:
            alive = [n.node_id for n in self.live_nodes()]
            if len(alive) > 1:
                victim = self.rng.choice(alive)
                self.crash(victim, restart_after=self.rng.randint(15, 90))
        elif fault is Fault.PAUSE:
            pausable = [n for n in self.live_nodes() if n.paused_until <= self.now]
            if pausable:
                victim_node = self.rng.choice(pausable)
                victim_node.paused_until = self.now + self.rng.randint(15, 60)
                self._log(f"pause n{victim_node.node_id} until t={victim_node.paused_until}")
        elif fault is Fault.LOSS_BURST:
            self.net.drop_rate = 0.3
            self.loss_burst_until = self.now + 40
            self._log("packet loss burst 30%")
        elif fault is Fault.TRANSFER:
            self._transfer_leadership()
        elif fault is Fault.MEMBERSHIP:
            self._membership_step()
        elif fault is Fault.JOINT_RECONFIG:
            self._joint_reconfig_step()

    def _current_members(self) -> set[int]:
        ldr = self.leader()
        if ldr is not None and ldr.raft is not None:
            return set(ldr.raft.membership.members)
        return {n.node_id for n in self.nodes.values() if not n.decommissioned}

    def _transfer_leadership(self) -> None:
        ldr = self.leader()
        if ldr is None or ldr.raft is None:
            return
        others = sorted(ldr.raft.membership.voters - {ldr.node_id})
        if not others:
            return
        target = self.rng.choice(others)
        try:
            ldr.raft.transfer_leadership(target)
            self._log(f"transfer leadership n{ldr.node_id} -> n{target}")
        except (NotLeaderError, ValueError, KeyError):
            return

    def _membership_step(self) -> None:
        ldr = self.leader()
        if ldr is None or ldr.raft is None:
            return
        r = ldr.raft
        voters = r.membership.voters
        learners = r.membership.learners
        try:
            if learners:
                learner = min(learners)
                pr = r.progress.get(learner)
                if pr is not None and pr.match >= r.log.committed:
                    r.propose_conf_change(ConfChange(ConfChangeType.PROMOTE, learner))
                    self.membership_changes += 1
                    self._log(f"promote learner n{learner}")
            elif len(voters) >= 5 or (len(voters) > 3 and self.rng.random() < 0.5):
                victim = self.rng.choice(sorted(voters - {r.id}))
                r.propose_conf_change(ConfChange(ConfChangeType.REMOVE, victim))
                self.membership_changes += 1
                self._removed_pending.add(victim)
                self._log(f"remove voter n{victim}")
            else:
                nid = self.next_node_id
                self.next_node_id += 1
                r.propose_conf_change(ConfChange(ConfChangeType.ADD_LEARNER, nid))
                self._create_node(nid)
                self.membership_changes += 1
                self._log(f"add learner n{nid}")
        except (ProposalDroppedError, NotLeaderError, ValueError):
            return

    def _joint_reconfig_step(self) -> None:
        ldr = self.leader()
        if ldr is None or ldr.raft is None:
            return
        r = ldr.raft
        if r.membership.is_joint or len(r.membership.voters) < 3:
            return
        candidates = sorted(r.membership.voters - {r.id})
        if not candidates:
            return
        replace_count = 1 if len(candidates) == 1 else self.rng.randint(1, min(2, len(candidates)))
        outgoing = set(self.rng.sample(candidates, replace_count))
        incoming: set[int] = set()
        for _ in range(replace_count):
            nid = self.next_node_id
            self.next_node_id += 1
            incoming.add(nid)
        new_voters = (set(r.membership.voters) - outgoing) | incoming
        try:
            r.propose_joint_reconfig(new_voters)
            for nid in sorted(incoming):
                self._create_node(nid)
            self._removed_pending |= outgoing
            self.membership_changes += 1
            self._log(f"joint reconfig {sorted(r.membership.voters)} -> {sorted(new_voters)}")
        except (ProposalDroppedError, NotLeaderError, ValueError):
            return

    def _timed_recoveries(self) -> None:
        if self.loss_burst_until and self.now >= self.loss_burst_until:
            self.net.drop_rate = self.base_drop_rate
            self.loss_burst_until = 0
        for node in self.nodes.values():
            if not node.alive and node.restart_at and self.now >= node.restart_at:
                self.restart(node.node_id)
        self._decommission_removed()

    def _decommission_removed(self) -> None:
        if not self._removed_pending:
            return
        ldr = self.leader()
        if ldr is None or ldr.raft is None:
            return
        r = ldr.raft
        if r.membership.is_joint or r.pending_conf_index > r.log.committed:
            return
        for nid in sorted(list(self._removed_pending)):
            if nid not in r.membership.members:
                node = self.nodes[nid]
                node.decommissioned = True
                if node.alive:
                    node.raft = None
                    node.incarnation += 1
                self._removed_pending.discard(nid)
                self._log(f"decommission n{nid}")

    def _heal_everything(self) -> None:
        self.net.heal()
        self.net.drop_rate = self.base_drop_rate
        self.loss_burst_until = 0
        for node in self.nodes.values():
            node.paused_until = 0
            if not node.alive and not node.decommissioned:
                self.restart(node.node_id)
        self._log("final heal: network healed, all nodes running")

    # ======================================================================= report
    def _report(self) -> SimReport:
        ops: list[Operation] = []
        ok = failed = info = 0
        for i, h in enumerate(self.history):
            if h.status == "ok":
                ok += 1
            elif h.status == "info":
                info += 1
            else:
                failed += 1
                continue
            if h.kind is OpKind.READ and h.status != "ok":
                continue
            ops.append(
                Operation(
                    op_id=i,
                    process=h.process,
                    kind=h.kind,
                    key=h.key,
                    value=h.value,
                    call=h.call,
                    ret=h.ret if h.status == "ok" else None,
                    expected=h.expected,
                    ok=h.ok if h.status == "ok" else None,
                )
            )
        lin = check_history(ops)
        settle_start = self.cfg.ticks + self.cfg.raft.election_tick * 4
        liveness_ok = any(
            h.status == "ok" and h.kind is not OpKind.READ and h.call >= settle_start
            for h in self.history
        )
        max_term = max((n.raft.term for n in self.live_nodes() if n.raft), default=0)
        ldr = self.leader()
        voters = sorted(ldr.raft.membership.voters) if ldr and ldr.raft else []
        digest = hashlib.sha256()
        for h in self.history:
            digest.update(f"{h.kind}{h.key}{h.value}{h.call}{h.ret}{h.status}{h.ok};".encode())
        for nid in sorted(self.nodes):
            n = self.nodes[nid]
            digest.update(f"{nid}:{n.sm.digest() if n.alive else 'down'}|".encode())
        return SimReport(
            seed=self.cfg.seed,
            bugs=sorted(self.cfg.raft.bugs),
            ticks=self.now,
            violations=list(self.observer.violations),
            linearizability=lin,
            ops_ok=ok,
            ops_failed=failed,
            ops_indeterminate=info,
            faults_injected=dict(sorted(self.faults_injected.items())),
            elections=self.elections,
            max_term=max_term,
            snapshots_taken=self.snapshots_taken,
            snapshots_installed=self.snapshots_installed,
            membership_changes=self.membership_changes,
            crashes=self.crashes,
            restarts=self.restarts,
            network=self.net.stats,
            invariant_checks=self.observer.checks,
            liveness_ok=liveness_ok,
            final_voters=voters,
            fingerprint=digest.hexdigest()[:16],
            event_log=self.event_log,
        )


def run_simulation(cfg: SimConfig) -> SimReport:
    return SimCluster(cfg).run()
