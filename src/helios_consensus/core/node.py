"""Deterministic Raft consensus core (no I/O, no clocks, no threads).

Implements the Raft paper plus the production extensions from Ongaro's dissertation and
etcd/raft:

* PreVote (dissertation 9.6) and CheckQuorum / leader stickiness, so partitioned or
  restarted nodes cannot disrupt a healthy leader by inflating terms.
* Leader no-op on election and the section 5.4.2 rule: only entries from the leader's
  current term are committed by counting replicas.
* Fast log back-tracking using the follower's conflict-term hint.
* Probe / Replicate / Snapshot flow control with bounded in-flight appends.
* Log compaction and InstallSnapshot.
* Single-server membership changes with learners (add learner, promote, remove); at most
  one uncommitted change, and only after the leader has committed an entry in its term.
* Leadership transfer (TimeoutNow) and linearizable ReadIndex reads.

Mutation hooks (``RaftConfig.bugs``) deliberately break individual rules so the test
harness can prove its checkers detect each class of safety bug.
"""

from __future__ import annotations

import random
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from helios_consensus.core.log import RaftLog
from helios_consensus.core.messages import (
    CAMPAIGN_TRANSFER,
    LEADER_MESSAGES,
    VOTE_REQUESTS,
    ConfChange,
    Entry,
    EntryKind,
    HardState,
    Membership,
    Message,
    MsgType,
    ReadState,
    Ready,
    Snapshot,
)
from helios_consensus.core.progress import Progress, ProgressState

KNOWN_BUGS = frozenset(
    {
        "commit_prior_term",  # count replicas for entries from earlier terms (Figure 8)
        "no_election_restriction",  # grant votes to candidates with stale logs
        "double_vote",  # grant more than one vote per term
        "stale_read",  # serve ReadIndex without confirming leadership with a quorum
        "no_truncate_on_conflict",  # overwrite conflicting entries but keep stale suffix
        "volatile_vote",  # forget to persist votedFor before replying
    }
)


class NotLeaderError(Exception):
    def __init__(self, leader_hint: int | None) -> None:
        super().__init__(f"not leader (hint={leader_hint})")
        self.leader_hint = leader_hint


class ProposalDroppedError(Exception):
    """The leader cannot accept this proposal right now (transfer or pending config)."""


class StateRole(StrEnum):
    FOLLOWER = "FOLLOWER"
    PRE_CANDIDATE = "PRE_CANDIDATE"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


class _VoteResult(StrEnum):
    PENDING = "PENDING"
    WON = "WON"
    LOST = "LOST"


@dataclass(frozen=True, slots=True)
class RaftConfig:
    election_tick: int = 10
    heartbeat_tick: int = 2
    max_entries_per_msg: int = 32
    max_inflight: int = 64
    pre_vote: bool = True
    check_quorum: bool = True
    bugs: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if self.heartbeat_tick <= 0 or self.election_tick <= self.heartbeat_tick:
            raise ValueError("election_tick must be greater than heartbeat_tick > 0")
        unknown = self.bugs - KNOWN_BUGS
        if unknown:
            raise ValueError(f"unknown bug flags: {sorted(unknown)}")


@dataclass(slots=True)
class _PendingRead:
    request_id: int
    index: int
    acks: set[int] = field(default_factory=set)


class RaftNode:
    def __init__(
        self,
        node_id: int,
        config: RaftConfig,
        *,
        seed: int,
        snapshot: Snapshot,
        hard_state: HardState | None = None,
        entries: Sequence[Entry] = (),
    ) -> None:
        hs = hard_state or HardState()
        self.id = node_id
        self.cfg = config
        self._rng = random.Random(seed * 1_000_003 + node_id * 7_919)
        self.log = RaftLog(snapshot)
        self.log.allow_conflict_without_truncate = "no_truncate_on_conflict" in config.bugs
        self.log.load(list(entries), hs.commit)
        self.term = hs.term
        self.vote = hs.vote
        self.role = StateRole.FOLLOWER
        self.lead: int | None = None

        self.membership = snapshot.membership
        self.config_history: list[tuple[int, Membership]] = [(snapshot.index, snapshot.membership)]
        self._config_index = snapshot.index

        self.progress: dict[int, Progress] = {}
        self.votes: dict[int, bool] = {}
        self.pending_conf_index = 0
        self.lead_transferee: int | None = None

        self.election_elapsed = 0
        self.heartbeat_elapsed = 0
        self.randomized_timeout = config.election_tick
        self._reset_randomized_timeout()

        self.msgs: list[Message] = []
        self._read_states: list[ReadState] = []
        self._pending_reads: list[_PendingRead] = []
        self._queued_reads: list[int] = []
        self._prev_hs = HardState(
            term=hs.term, vote=None if "volatile_vote" in config.bugs else hs.vote, commit=hs.commit
        )
        self._apply_committed_config()

    # ======================================================================= public API
    def __repr__(self) -> str:
        return (
            f"RaftNode(id={self.id}, role={self.role}, term={self.term}, lead={self.lead}, "
            f"last={self.log.last_index}, commit={self.log.committed})"
        )

    def promotable(self) -> bool:
        return self.id in self.membership.voters

    def tick(self) -> None:
        if self.role is StateRole.LEADER:
            self._tick_heartbeat()
        else:
            self._tick_election()

    def campaign(self) -> None:
        """Start an election immediately (used by tests and operators)."""
        if self.promotable() and self.role is not StateRole.LEADER:
            self._campaign(pre=self.cfg.pre_vote)

    def propose(self, data: str) -> int:
        self._require_leader()
        if self.lead_transferee is not None:
            raise ProposalDroppedError("leadership transfer in progress")
        return self._append_entry(EntryKind.NORMAL, data)

    def propose_conf_change(self, cc: ConfChange) -> int:
        self._require_leader()
        if self.lead_transferee is not None:
            raise ProposalDroppedError("leadership transfer in progress")
        if self.pending_conf_index > self.log.committed:
            raise ProposalDroppedError("a configuration change is already pending")
        if self.log.term(self.log.committed) != self.term:
            raise ProposalDroppedError("leader has not committed an entry in its term yet")
        self.membership.apply(cc)  # validate against the committed configuration
        index = self._append_entry(EntryKind.CONFIG, cc.encode())
        self.pending_conf_index = index
        return index

    def read_index(self, request_id: int) -> None:
        """Request a linearizable read; a ``ReadState`` is emitted once it is safe."""
        self._require_leader()
        if request_id <= 0:
            raise ValueError("request_id must be positive")
        if "stale_read" in self.cfg.bugs:
            self._read_states.append(ReadState(request_id=request_id, index=self.log.committed))
            return
        if self.log.term(self.log.committed) != self.term:
            self._queued_reads.append(request_id)
            return
        self._start_read(request_id)

    def transfer_leadership(self, target: int) -> None:
        self._require_leader()
        if target == self.id or target not in self.membership.voters:
            raise ValueError(f"cannot transfer leadership to {target}")
        self.lead_transferee = target
        self.election_elapsed = 0
        pr = self.progress[target]
        if pr.match == self.log.last_index:
            self._send(Message(type=MsgType.TIMEOUT_NOW, src=self.id, dst=target, term=self.term))
        else:
            self._send_append(target)

    def compact(self, index: int, data: str) -> Snapshot:
        """Take a snapshot at ``index`` (must be applied) and discard earlier log entries."""
        term = self.log.term(index)
        if term < 0:
            raise ValueError(f"cannot snapshot unknown index {index}")
        snap = Snapshot(index=index, term=term, membership=self.membership_at(index), data=data)
        self.log.compact(snap)
        keep = [h for h in self.config_history if h[0] > index]
        self.config_history = [(index, snap.membership), *keep]
        return snap

    def membership_at(self, index: int) -> Membership:
        current = self.config_history[0][1]
        for idx, m in self.config_history:
            if idx <= index:
                current = m
        return current

    def has_ready(self) -> bool:
        hs = self._hard_state()
        return bool(
            hs != self._prev_hs
            or self.msgs
            or self._read_states
            or self.log.pending_snapshot is not None
            or self.log.unstable_entries()
            or self.log.next_committed_entries()
        )

    def ready(self) -> Ready:
        hs = self._hard_state()
        rd = Ready(
            hard_state=hs if hs != self._prev_hs else None,
            entries=self.log.unstable_entries(),
            snapshot=self.log.pending_snapshot,
            committed_entries=self.log.next_committed_entries(),
            messages=self.msgs,
            read_states=self._read_states,
        )
        self.msgs = []
        self._read_states = []
        return rd

    def advance(self, rd: Ready) -> None:
        if rd.hard_state is not None:
            self._prev_hs = rd.hard_state
        if rd.entries:
            self.log.stable_to(rd.entries[-1].index)
        if rd.snapshot is not None:
            self.log.pending_snapshot = None
            self.log.applied_to(rd.snapshot.index)
        if rd.committed_entries:
            self.log.applied_to(rd.committed_entries[-1].index)

    def status(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "role": self.role.value,
            "term": self.term,
            "vote": self.vote,
            "lead": self.lead,
            "first_index": self.log.first_index,
            "last_index": self.log.last_index,
            "commit": self.log.committed,
            "applied": self.log.applied,
            "voters": sorted(self.membership.voters),
            "learners": sorted(self.membership.learners),
            "progress": {
                peer: {"match": pr.match, "next": pr.next, "state": pr.state.value}
                for peer, pr in sorted(self.progress.items())
            },
        }

    # ======================================================================= step
    def step(self, m: Message) -> None:
        if m.dst != self.id:
            raise ValueError(f"message for {m.dst} delivered to {self.id}")

        if m.term > self.term:
            if m.type in VOTE_REQUESTS:
                force = m.context == CAMPAIGN_TRANSFER
                in_lease = (
                    self.cfg.check_quorum
                    and self.lead is not None
                    and self.election_elapsed < self.cfg.election_tick
                )
                if in_lease and not force:
                    return  # leader stickiness: a live leader exists, ignore disruptive voters
            if m.type is MsgType.PRE_VOTE or (m.type is MsgType.PRE_VOTE_RESP and not m.reject):
                pass  # pre-votes never change our term
            else:
                lead = m.src if m.type in LEADER_MESSAGES else None
                self._become_follower(m.term, lead)
        elif m.term < self.term:
            if m.type in (MsgType.APPEND, MsgType.HEARTBEAT) and (
                self.cfg.check_quorum or self.cfg.pre_vote
            ):
                # Tell the stale leader about the newer term so it steps down.
                self._send(
                    Message(type=MsgType.APPEND_RESP, src=self.id, dst=m.src, term=self.term)
                )
            elif m.type is MsgType.PRE_VOTE:
                self._send(
                    Message(
                        type=MsgType.PRE_VOTE_RESP,
                        src=self.id,
                        dst=m.src,
                        term=self.term,
                        reject=True,
                    )
                )
            return

        if m.type in VOTE_REQUESTS:
            self._handle_vote_request(m)
            return

        if self.role is StateRole.LEADER:
            self._step_leader(m)
        elif self.role in (StateRole.CANDIDATE, StateRole.PRE_CANDIDATE):
            self._step_candidate(m)
        else:
            self._step_follower(m)

    def _handle_vote_request(self, m: Message) -> None:
        resp_type = MsgType.PRE_VOTE_RESP if m.type is MsgType.PRE_VOTE else MsgType.VOTE_RESP
        can_vote = (
            self.vote == m.src
            or (self.vote is None and self.lead is None)
            or (m.type is MsgType.PRE_VOTE and m.term > self.term)
        )
        if "double_vote" in self.cfg.bugs and m.type is MsgType.VOTE:
            can_vote = True
        up_to_date = self.log.is_up_to_date(m.index, m.log_term)
        if "no_election_restriction" in self.cfg.bugs:
            up_to_date = True
        if can_vote and up_to_date:
            if m.type is MsgType.VOTE:
                self.election_elapsed = 0
                self.vote = m.src
            self._send(Message(type=resp_type, src=self.id, dst=m.src, term=m.term))
        else:
            self._send(Message(type=resp_type, src=self.id, dst=m.src, term=self.term, reject=True))

    def _step_leader(self, m: Message) -> None:
        if m.type is MsgType.APPEND_RESP:
            self._handle_append_resp(m)
        elif m.type is MsgType.HEARTBEAT_RESP:
            self._handle_heartbeat_resp(m)
        # APPEND/HEARTBEAT/SNAPSHOT at our own term would imply two leaders; ignore and
        # let the external invariant checker flag Election Safety.

    def _step_candidate(self, m: Message) -> None:
        if m.type in LEADER_MESSAGES:
            self._become_follower(self.term, m.src)
            self._step_follower(m)
            return
        expected = (
            MsgType.PRE_VOTE_RESP if self.role is StateRole.PRE_CANDIDATE else MsgType.VOTE_RESP
        )
        if m.type is not expected:
            return
        result = self._poll(m.src, granted=not m.reject)
        if result is _VoteResult.WON:
            if self.role is StateRole.PRE_CANDIDATE:
                self._campaign(pre=False, transfer=False)
            else:
                self._become_leader()
        elif result is _VoteResult.LOST:
            self._become_follower(self.term, None)

    def _step_follower(self, m: Message) -> None:
        if m.type is MsgType.APPEND:
            self.election_elapsed = 0
            self.lead = m.src
            self._handle_append(m)
        elif m.type is MsgType.HEARTBEAT:
            self.election_elapsed = 0
            self.lead = m.src
            self._handle_heartbeat(m)
        elif m.type is MsgType.SNAPSHOT:
            self.election_elapsed = 0
            self.lead = m.src
            self._handle_snapshot(m)
        elif m.type is MsgType.TIMEOUT_NOW and self.promotable():
            self._campaign(pre=False, transfer=True)

    # ======================================================================= follower side
    def _handle_append(self, m: Message) -> None:
        if m.index < self.log.committed:
            self._reply_append(m.src, index=self.log.committed)
            return
        last_new = self.log.maybe_append(m.index, m.log_term, m.commit, m.entries)
        if last_new is not None:
            self._on_commit_advanced()
            self._reply_append(m.src, index=last_new)
            return
        hint_index = self.log.find_conflict_by_term(min(m.index, self.log.last_index), m.log_term)
        self._send(
            Message(
                type=MsgType.APPEND_RESP,
                src=self.id,
                dst=m.src,
                term=self.term,
                index=m.index,
                reject=True,
                reject_hint=hint_index,
                log_term=self.log.term(hint_index),
            )
        )

    def _handle_heartbeat(self, m: Message) -> None:
        if self.log.commit_to(min(m.commit, self.log.last_index)):
            self._on_commit_advanced()
        self._send(
            Message(
                type=MsgType.HEARTBEAT_RESP,
                src=self.id,
                dst=m.src,
                term=self.term,
                context=m.context,
            )
        )

    def _handle_snapshot(self, m: Message) -> None:
        snap = m.snapshot
        if snap is None:
            return
        if snap.index <= self.log.committed:
            self._reply_append(m.src, index=self.log.committed)
            return
        if self.log.match_term(snap.index, snap.term):
            self.log.commit_to(snap.index)
            self._on_commit_advanced()
            self._reply_append(m.src, index=snap.index)
            return
        self.log.restore(snap)
        self.membership = snap.membership
        self.config_history = [(snap.index, snap.membership)]
        self._config_index = snap.index
        self._reply_append(m.src, index=snap.index)

    def _reply_append(self, to: int, *, index: int) -> None:
        self._send(
            Message(type=MsgType.APPEND_RESP, src=self.id, dst=to, term=self.term, index=index)
        )

    # ======================================================================= leader side
    def _handle_append_resp(self, m: Message) -> None:
        pr = self.progress.get(m.src)
        if pr is None:
            return
        pr.recent_active = True
        if m.reject:
            next_probe = m.reject_hint
            if m.log_term > 0:
                next_probe = self.log.find_conflict_by_term(m.reject_hint, m.log_term)
            if pr.maybe_decrease(m.index, next_probe):
                if pr.state is ProgressState.REPLICATE:
                    pr.become_probe()
                self._send_append(m.src, allow_empty=True)
            return

        if pr.maybe_update(m.index):
            if pr.state is ProgressState.PROBE:
                pr.become_replicate()
            elif pr.state is ProgressState.SNAPSHOT and pr.match >= pr.pending_snapshot:
                pr.become_probe()
            if self._maybe_commit():
                self._bcast_append()
        self._send_while_possible(m.src)
        if m.src == self.lead_transferee and pr.match == self.log.last_index:
            self._send(Message(type=MsgType.TIMEOUT_NOW, src=self.id, dst=m.src, term=self.term))

    def _handle_heartbeat_resp(self, m: Message) -> None:
        pr = self.progress.get(m.src)
        if pr is None:
            return
        pr.recent_active = True
        pr.paused = False
        if pr.state is ProgressState.REPLICATE and len(pr.inflight) >= self.cfg.max_inflight:
            pr.inflight.pop(0)
        if pr.match < self.log.last_index:
            self._send_append(m.src, allow_empty=True)
        if m.context > 0:
            self._ack_read(m.context, m.src)

    def _send_while_possible(self, to: int) -> None:
        pr = self.progress[to]
        for _ in range(self.cfg.max_inflight):
            if pr.is_paused(self.cfg.max_inflight) or pr.next > self.log.last_index:
                return
            if not self._send_append(to):
                return

    def _send_append(self, to: int, *, allow_empty: bool = False) -> bool:
        pr = self.progress.get(to)
        if pr is None or pr.state is ProgressState.SNAPSHOT:
            return False
        prev = pr.next - 1
        prev_term = self.log.term(prev)
        if prev_term < 0:
            snap = self.log.snapshot
            self._send(
                Message(type=MsgType.SNAPSHOT, src=self.id, dst=to, term=self.term, snapshot=snap)
            )
            pr.become_snapshot(snap.index)
            return True
        entries = self.log.slice(pr.next, pr.next + self.cfg.max_entries_per_msg)
        if not entries and not allow_empty:
            return False
        self._send(
            Message(
                type=MsgType.APPEND,
                src=self.id,
                dst=to,
                term=self.term,
                index=prev,
                log_term=prev_term,
                entries=tuple(entries),
                commit=self.log.committed,
            )
        )
        if entries:
            pr.sent_entries(entries[-1].index)
        elif pr.state is ProgressState.PROBE:
            pr.paused = True
        return True

    def _bcast_append(self) -> None:
        for peer in sorted(self.progress):
            if peer == self.id:
                continue
            pr = self.progress[peer]
            if not pr.is_paused(self.cfg.max_inflight):
                self._send_append(peer, allow_empty=True)

    def _bcast_heartbeat(self) -> None:
        context = self._pending_reads[-1].request_id if self._pending_reads else 0
        for peer in sorted(self.progress):
            if peer == self.id:
                continue
            pr = self.progress[peer]
            self._send(
                Message(
                    type=MsgType.HEARTBEAT,
                    src=self.id,
                    dst=peer,
                    term=self.term,
                    commit=min(pr.match, self.log.committed),
                    context=context,
                )
            )

    def _maybe_commit(self) -> bool:
        voters = self.membership.voters
        matches = sorted(
            (self.progress[v].match for v in voters if v in self.progress), reverse=True
        )
        quorum = self.membership.quorum()
        if len(matches) < quorum:
            return False
        index = matches[quorum - 1]
        if index <= self.log.committed:
            return False
        if self.log.term(index) != self.term and "commit_prior_term" not in self.cfg.bugs:
            return False  # Raft section 5.4.2: never commit prior-term entries by counting
        self.log.commit_to(index)
        self._on_commit_advanced()
        return True

    def _append_entry(self, kind: EntryKind, data: str) -> int:
        e = Entry(index=self.log.last_index + 1, term=self.term, kind=kind, data=data)
        self.log.append([e])
        self.progress[self.id].maybe_update(e.index)
        self._maybe_commit()
        self._bcast_append()
        return e.index

    def _quorum_active(self) -> bool:
        active = 0
        for peer, pr in self.progress.items():
            if peer == self.id:
                active += peer in self.membership.voters
                continue
            if pr.recent_active and peer in self.membership.voters:
                active += 1
            pr.recent_active = False
        return active >= self.membership.quorum()

    # ======================================================================= reads
    def _start_read(self, request_id: int) -> None:
        pending = _PendingRead(request_id=request_id, index=self.log.committed, acks={self.id})
        self._pending_reads.append(pending)
        if len(pending.acks & self.membership.voters) >= self.membership.quorum():
            self._release_reads_through(len(self._pending_reads) - 1)
        else:
            self._bcast_heartbeat()

    def _ack_read(self, request_id: int, src: int) -> None:
        for pos, pending in enumerate(self._pending_reads):
            if pending.request_id == request_id:
                pending.acks.add(src)
                if len(pending.acks & self.membership.voters) >= self.membership.quorum():
                    self._release_reads_through(pos)
                return

    def _release_reads_through(self, pos: int) -> None:
        released, self._pending_reads = (
            self._pending_reads[: pos + 1],
            self._pending_reads[pos + 1 :],
        )
        for pending in released:
            self._read_states.append(ReadState(request_id=pending.request_id, index=pending.index))

    # ======================================================================= transitions
    def _tick_election(self) -> None:
        self.election_elapsed += 1
        if self.promotable() and self.election_elapsed >= self.randomized_timeout:
            self.election_elapsed = 0
            self._campaign(pre=self.cfg.pre_vote)

    def _tick_heartbeat(self) -> None:
        self.heartbeat_elapsed += 1
        self.election_elapsed += 1
        if self.election_elapsed >= self.cfg.election_tick:
            self.election_elapsed = 0
            if self.cfg.check_quorum and not self._quorum_active():
                self._become_follower(self.term, None)
                return
            self.lead_transferee = None  # abort a transfer that did not finish in time
        for pr in self.progress.values():
            if pr.state is ProgressState.SNAPSHOT:
                pr.snapshot_wait += 1
                if pr.snapshot_wait >= self.cfg.election_tick:
                    pr.become_probe()  # snapshot was probably lost; retry
        if self.heartbeat_elapsed >= self.cfg.heartbeat_tick:
            self.heartbeat_elapsed = 0
            self._bcast_heartbeat()

    def _campaign(self, *, pre: bool, transfer: bool = False) -> None:
        if pre:
            self.role = StateRole.PRE_CANDIDATE
            self.lead = None
            self.votes = {}
            vote_term = self.term + 1
            msg_type = MsgType.PRE_VOTE
        else:
            self.term += 1
            self.vote = self.id
            self.role = StateRole.CANDIDATE
            self.lead = None
            self.votes = {}
            vote_term = self.term
            msg_type = MsgType.VOTE
        self.election_elapsed = 0
        self._reset_randomized_timeout()
        if self._poll(self.id, granted=True) is _VoteResult.WON:
            if pre:
                self._campaign(pre=False, transfer=transfer)
            else:
                self._become_leader()
            return
        for peer in sorted(self.membership.voters - {self.id}):
            self._send(
                Message(
                    type=msg_type,
                    src=self.id,
                    dst=peer,
                    term=vote_term,
                    index=self.log.last_index,
                    log_term=self.log.last_term(),
                    context=CAMPAIGN_TRANSFER if transfer else 0,
                )
            )

    def _poll(self, src: int, *, granted: bool) -> _VoteResult:
        self.votes.setdefault(src, granted)
        voters = self.membership.voters
        yes = sum(1 for v in voters if self.votes.get(v) is True)
        no = sum(1 for v in voters if self.votes.get(v) is False)
        quorum = self.membership.quorum()
        if yes >= quorum:
            return _VoteResult.WON
        if no > len(voters) - quorum:
            return _VoteResult.LOST
        return _VoteResult.PENDING

    def _become_follower(self, term: int, lead: int | None) -> None:
        if term != self.term:
            self.term = term
            self.vote = None
        self.role = StateRole.FOLLOWER
        self.lead = lead
        self.election_elapsed = 0
        self.heartbeat_elapsed = 0
        self.votes = {}
        self.progress = {}
        self.lead_transferee = None
        self._pending_reads.clear()
        self._queued_reads.clear()
        self._reset_randomized_timeout()

    def _become_leader(self) -> None:
        self.role = StateRole.LEADER
        self.lead = self.id
        self.lead_transferee = None
        self.election_elapsed = 0
        self.heartbeat_elapsed = 0
        self.progress = {}
        last = self.log.last_index
        for member in sorted(self.membership.members | {self.id}):
            pr = Progress(match=0, next=last + 1)
            if member == self.id:
                pr.match = last
                pr.state = ProgressState.REPLICATE
                pr.recent_active = True
            self.progress[member] = pr
        noop = Entry(index=last + 1, term=self.term, kind=EntryKind.NOOP)
        self.log.append([noop])
        self.progress[self.id].maybe_update(noop.index)
        self.pending_conf_index = noop.index
        self._maybe_commit()
        self._bcast_append()

    def _on_commit_advanced(self) -> None:
        self._apply_committed_config()
        if (
            self.role is StateRole.LEADER
            and self._queued_reads
            and self.log.term(self.log.committed) == self.term
        ):
            queued, self._queued_reads = self._queued_reads, []
            for rid in queued:
                self._start_read(rid)

    def _apply_committed_config(self) -> None:
        changed = False
        for idx in range(max(self._config_index + 1, self.log.first_index), self.log.committed + 1):
            e = self.log.entry(idx)
            if e.kind is EntryKind.CONFIG:
                self.membership = self.membership.apply(ConfChange.decode(e.data))
                self.config_history.append((idx, self.membership))
                changed = True
        self._config_index = max(self._config_index, self.log.committed)
        if changed and self.role is StateRole.LEADER:
            self._sync_progress_with_membership()

    def _sync_progress_with_membership(self) -> None:
        members = self.membership.members
        for peer in list(self.progress):
            if peer not in members and peer != self.id:
                del self.progress[peer]
        for peer in sorted(members):
            if peer not in self.progress:
                self.progress[peer] = Progress(match=0, next=self.log.last_index + 1)
        if self.lead_transferee is not None and self.lead_transferee not in self.membership.voters:
            self.lead_transferee = None
        if self.id not in self.membership.voters:
            self._become_follower(self.term, None)

    # ======================================================================= helpers
    def _require_leader(self) -> None:
        if self.role is not StateRole.LEADER:
            raise NotLeaderError(self.lead)

    def _send(self, m: Message) -> None:
        self.msgs.append(m)

    def _reset_randomized_timeout(self) -> None:
        tick = self.cfg.election_tick
        self.randomized_timeout = tick + self._rng.randrange(tick)

    def _hard_state(self) -> HardState:
        vote = None if "volatile_vote" in self.cfg.bugs else self.vote
        return HardState(term=self.term, vote=vote, commit=self.log.committed)
