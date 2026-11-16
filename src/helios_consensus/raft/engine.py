"""Deterministic Simulation Testing (`DST`) Raft Replicated State Machine (Ongaro & Ousterhout, 2014).

Models a 5- or 7-node asynchronous message-passing Raft cluster on a deterministic logical tick
clock—supporting asymmetric network partitions, minority leader isolation, log backtracking, and
Ongaro §5.4.2 current-term commit safety—asserting `ElectionSafety` and `LogMatching` at every tick.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from enum import StrEnum

from helios_consensus.types import RaftDSTReport, RaftTimelineEvent


class NodeRole(StrEnum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


@dataclass(frozen=True)
class LogEntry:
    term: int
    command: str


@dataclass(frozen=True)
class RequestVoteRPC:
    src: int
    dst: int
    term: int
    last_log_index: int
    last_log_term: int


@dataclass(frozen=True)
class RequestVoteReply:
    src: int
    dst: int
    term: int
    vote_granted: bool


@dataclass(frozen=True)
class AppendEntriesRPC:
    src: int
    dst: int
    term: int
    prev_log_index: int
    prev_log_term: int
    entries: tuple[LogEntry, ...]
    leader_commit: int


@dataclass(frozen=True)
class AppendEntriesReply:
    src: int
    dst: int
    term: int
    success: bool
    match_index: int


RaftMsg = RequestVoteRPC | RequestVoteReply | AppendEntriesRPC | AppendEntriesReply


@dataclass
class RaftNode:
    node_id: int
    cluster_size: int
    election_timeout_base: int
    current_term: int = 0
    voted_for: int | None = None
    role: NodeRole = NodeRole.FOLLOWER
    log: list[LogEntry] = field(default_factory=lambda: [LogEntry(term=0, command="SENTINEL")])
    commit_index: int = 0
    ticks_since_heard: int = 0
    votes_received: set[int] = field(default_factory=set)
    next_index: dict[int, int] = field(default_factory=dict)
    match_index: dict[int, int] = field(default_factory=dict)

    def last_log_index(self) -> int:
        return len(self.log) - 1

    def last_log_term(self) -> int:
        return self.log[-1].term

    def quorum_size(self) -> int:
        return (self.cluster_size // 2) + 1


class RaftClusterSimulator:
    """Deterministic logical-tick Raft cluster simulator with partition matrix and safety checks."""

    def __init__(self, cluster_size: int = 5) -> None:
        if cluster_size < 3 or cluster_size % 2 == 0:
            raise ValueError("cluster_size must be odd and >= 3")
        self.cluster_size = cluster_size
        # Staggered deterministic election timeouts prevent perpetual split-vote livelock
        self.nodes = [
            RaftNode(node_id=i, cluster_size=cluster_size, election_timeout_base=6 + i * 2)
            for i in range(cluster_size)
        ]
        self.tick_now = 0
        self.queue: list[tuple[int, RaftMsg]] = []
        self.blocked_links: set[tuple[int, int]] = set()
        self.total_routed = 0
        self.dropped_msgs = 0
        self.elections_won = 0
        self.leaders_by_term: dict[int, int] = {}
        self.timeline: list[RaftTimelineEvent] = []

    def set_partition(self, group_a: set[int], group_b: set[int]) -> None:
        self.blocked_links.clear()
        for a in group_a:
            for b in group_b:
                self.blocked_links.add((a, b))
                self.blocked_links.add((b, a))
        self.timeline.append(
            RaftTimelineEvent(
                tick=self.tick_now,
                event_type="NETWORK_PARTITION",
                detail=f"Isolated {sorted(group_a)} <-> {sorted(group_b)}",
            )
        )

    def heal_partition(self) -> None:
        self.blocked_links.clear()
        self.timeline.append(
            RaftTimelineEvent(
                tick=self.tick_now,
                event_type="PARTITION_HEALED",
                detail="Full mesh connectivity restored across all Raft nodes",
            )
        )

    def current_leader(self) -> RaftNode | None:
        leaders = [n for n in self.nodes if n.role == NodeRole.LEADER]
        if not leaders:
            return None
        return max(leaders, key=lambda n: n.current_term)

    def submit_client_command(self, command: str, target_leader_id: int | None = None) -> bool:
        node = (
            self.nodes[target_leader_id] if target_leader_id is not None else self.current_leader()
        )
        if node is None or node.role != NodeRole.LEADER:
            return False
        node.log.append(LogEntry(term=node.current_term, command=command))
        node.match_index[node.node_id] = node.last_log_index()
        self._broadcast_append_entries(node)
        return True

    def _send(self, msg: RaftMsg, delay: int = 1) -> None:
        if (msg.src, msg.dst) in self.blocked_links:
            self.dropped_msgs += 1
            return
        self.total_routed += 1
        self.queue.append((self.tick_now + delay, msg))

    def _start_election(self, node: RaftNode) -> None:
        node.current_term += 1
        node.role = NodeRole.CANDIDATE
        node.voted_for = node.node_id
        node.votes_received = {node.node_id}
        node.ticks_since_heard = 0
        for peer in range(self.cluster_size):
            if peer != node.node_id:
                self._send(
                    RequestVoteRPC(
                        src=node.node_id,
                        dst=peer,
                        term=node.current_term,
                        last_log_index=node.last_log_index(),
                        last_log_term=node.last_log_term(),
                    )
                )

    def _broadcast_append_entries(self, leader: RaftNode) -> None:
        for peer in range(self.cluster_size):
            if peer == leader.node_id:
                continue
            raw_nxt = leader.next_index.get(peer, leader.last_log_index() + 1)
            nxt = max(1, min(raw_nxt, leader.last_log_index() + 1))
            prev_idx = nxt - 1
            prev_term = leader.log[prev_idx].term
            entries = tuple(leader.log[prev_idx + 1 :])
            self._send(
                AppendEntriesRPC(
                    src=leader.node_id,
                    dst=peer,
                    term=leader.current_term,
                    prev_log_index=prev_idx,
                    prev_log_term=prev_term,
                    entries=entries,
                    leader_commit=leader.commit_index,
                )
            )

    def _step_down_if_higher_term(self, node: RaftNode, msg_term: int) -> None:
        if msg_term > node.current_term:
            node.current_term = msg_term
            node.role = NodeRole.FOLLOWER
            node.voted_for = None
            node.votes_received.clear()

    def _deliver(self, msg: RaftMsg) -> None:
        node = self.nodes[msg.dst]
        self._step_down_if_higher_term(node, msg.term)

        if isinstance(msg, RequestVoteRPC):
            grant = False
            if msg.term == node.current_term and node.voted_for in (None, msg.src):
                # Ongaro §5.4.1 Election Restriction: candidate log must be at least as up-to-date
                cand_key = (msg.last_log_term, msg.last_log_index)
                my_key = (node.last_log_term(), node.last_log_index())
                if cand_key >= my_key:
                    grant = True
                    node.voted_for = msg.src
                    node.ticks_since_heard = 0
            self._send(
                RequestVoteReply(
                    src=node.node_id, dst=msg.src, term=node.current_term, vote_granted=grant
                )
            )

        elif isinstance(msg, RequestVoteReply):
            if (
                node.role == NodeRole.CANDIDATE
                and msg.term == node.current_term
                and msg.vote_granted
            ):
                node.votes_received.add(msg.src)
                if len(node.votes_received) >= node.quorum_size():
                    node.role = NodeRole.LEADER
                    self.elections_won += 1
                    if node.current_term in self.leaders_by_term:
                        assert self.leaders_by_term[node.current_term] == node.node_id
                    self.leaders_by_term[node.current_term] = node.node_id
                    for p in range(self.cluster_size):
                        node.next_index[p] = node.last_log_index() + 1
                        node.match_index[p] = node.last_log_index() if p == node.node_id else 0
                    self.timeline.append(
                        RaftTimelineEvent(
                            tick=self.tick_now,
                            event_type="LEADER_ELECTED",
                            detail=f"Node n{node.node_id} elected LEADER for term {node.current_term}",
                        )
                    )
                    self._broadcast_append_entries(node)

        elif isinstance(msg, AppendEntriesRPC):
            if msg.term < node.current_term:
                self._send(
                    AppendEntriesReply(
                        src=node.node_id,
                        dst=msg.src,
                        term=node.current_term,
                        success=False,
                        match_index=0,
                    )
                )
                return
            node.role = NodeRole.FOLLOWER
            node.ticks_since_heard = 0
            if (
                msg.prev_log_index >= len(node.log)
                or node.log[msg.prev_log_index].term != msg.prev_log_term
            ):
                self._send(
                    AppendEntriesReply(
                        src=node.node_id,
                        dst=msg.src,
                        term=node.current_term,
                        success=False,
                        match_index=0,
                    )
                )
                return

            # Reconcile log suffix from prev_log_index + 1
            insert_pos = msg.prev_log_index + 1
            for entry in msg.entries:
                if insert_pos < len(node.log):
                    if node.log[insert_pos].term != entry.term:
                        del node.log[insert_pos:]
                        node.log.append(entry)
                else:
                    node.log.append(entry)
                insert_pos += 1

            acked_index = msg.prev_log_index + len(msg.entries)
            if msg.leader_commit > node.commit_index:
                node.commit_index = min(msg.leader_commit, acked_index)

            self._send(
                AppendEntriesReply(
                    src=node.node_id,
                    dst=msg.src,
                    term=node.current_term,
                    success=True,
                    match_index=acked_index,
                )
            )

        elif isinstance(msg, AppendEntriesReply):
            if node.role != NodeRole.LEADER or msg.term != node.current_term:
                return
            if msg.success:
                node.match_index[msg.src] = max(node.match_index.get(msg.src, 0), msg.match_index)
                node.next_index[msg.src] = node.match_index[msg.src] + 1
                # Ongaro §5.4.2: ONLY advance commit_index on entries from leader's CURRENT term!
                for idx in range(node.last_log_index(), node.commit_index, -1):
                    if node.log[idx].term != node.current_term:
                        continue
                    repl = sum(
                        1 for p in range(self.cluster_size) if node.match_index.get(p, 0) >= idx
                    )
                    if repl >= node.quorum_size():
                        node.commit_index = idx
                        break
            else:
                node.next_index[msg.src] = max(1, node.next_index.get(msg.src, 2) - 1)

    def verify_safety_invariants(self) -> tuple[bool, bool]:
        # 1. Election Safety: at most one leader per term
        seen_term_leaders: dict[int, int] = {}
        for n in self.nodes:
            if n.role == NodeRole.LEADER:
                if (
                    n.current_term in seen_term_leaders
                    and seen_term_leaders[n.current_term] != n.node_id
                ):
                    return False, False
                seen_term_leaders[n.current_term] = n.node_id

        # 2. Log Matching + Committed State Machine Safety
        max_committed = max(n.commit_index for n in self.nodes)
        for idx in range(1, max_committed + 1):
            committed_entries = [
                n.log[idx] for n in self.nodes if n.commit_index >= idx and idx < len(n.log)
            ]
            if len(set(committed_entries)) > 1:
                return True, False

        for i in range(self.cluster_size):
            for j in range(i + 1, self.cluster_size):
                li, lj = self.nodes[i].log, self.nodes[j].log
                limit = min(len(li), len(lj))
                for k in range(limit):
                    if li[k].term == lj[k].term and li[: k + 1] != lj[: k + 1]:
                        return True, False
        return True, True

    def step_ticks(self, count: int = 1) -> tuple[bool, bool]:
        elec_ok, log_ok = True, True
        for _ in range(count):
            self.tick_now += 1
            due = [m for t, m in self.queue if t <= self.tick_now]
            self.queue = [(t, m) for t, m in self.queue if t > self.tick_now]
            for m in due:
                self._deliver(m)

            for node in self.nodes:
                if node.role == NodeRole.LEADER:
                    if self.tick_now % 2 == 0:
                        self._broadcast_append_entries(node)
                else:
                    node.ticks_since_heard += 1
                    if node.ticks_since_heard >= node.election_timeout_base:
                        self._start_election(node)

            e_ok, l_ok = self.verify_safety_invariants()
            elec_ok = elec_ok and e_ok
            log_ok = log_ok and l_ok
        return elec_ok, log_ok


def run_raft_dst_partition_benchmark(cluster_size: int = 5) -> RaftDSTReport:
    """Execute a full deterministic Split-Brain Partition + Stale-Leader Uncommitted Write +
    Majority Failover + Partition Heal scenario, proving zero split-brain commits!
    """
    sim = RaftClusterSimulator(cluster_size=cluster_size)
    e1, l1 = sim.step_ticks(12)

    leader0 = sim.current_leader()
    assert leader0 is not None
    old_leader_id = leader0.node_id

    sim.submit_client_command("SET vault_limit=500000")
    sim.submit_client_command("SET quorum_mode=STRICT")
    e2, l2 = sim.step_ticks(6)

    # Isolate old leader + 1 peer (minority of 2) from majority of 3!
    minority = {old_leader_id, (old_leader_id + 1) % cluster_size}
    majority = set(range(cluster_size)) - minority
    sim.set_partition(minority, majority)

    # Submit stale command to isolated old leader -> MUST NEVER commit!
    sim.submit_client_command(
        "SET rogue_split_brain_transfer=999999", target_leader_id=old_leader_id
    )
    e3, l3 = sim.step_ticks(18)

    # Majority elects a new higher-term leader and commits legitimate commands
    new_leader = max(
        (sim.nodes[i] for i in majority if sim.nodes[i].role == NodeRole.LEADER),
        key=lambda n: n.current_term,
    )
    sim.submit_client_command("SET failover_epoch=VERIFIED", target_leader_id=new_leader.node_id)
    sim.submit_client_command("SET treasury_lock=ACTIVE", target_leader_id=new_leader.node_id)
    e4, l4 = sim.step_ticks(8)

    # Heal partition -> isolated stale leader steps down and overwrites uncommitted rogue entry!
    sim.heal_partition()
    e5, l5 = sim.step_ticks(10)

    # Verify all nodes converged to identical committed log WITHOUT rogue_split_brain_transfer!
    ref_committed = sim.nodes[0].log[1 : sim.nodes[0].commit_index + 1]
    for n in sim.nodes:
        assert n.log[1 : n.commit_index + 1] == ref_committed
    assert all("rogue_split_brain" not in entry.command for entry in ref_committed)

    digest = hashlib.sha256(
        "|".join(f"{e.term}:{e.command}" for e in ref_committed).encode()
    ).hexdigest()[:16]

    return RaftDSTReport(
        cluster_size=cluster_size,
        total_ticks=sim.tick_now,
        total_messages_routed=sim.total_routed,
        dropped_partition_messages=sim.dropped_msgs,
        elections_completed=sim.elections_won,
        committed_log_entries=len(ref_committed),
        election_safety_verified=all([e1, e2, e3, e4, e5]),
        log_matching_verified=all([l1, l2, l3, l4, l5]),
        linearizable_state_hash=digest,
        timeline=sim.timeline,
    )
