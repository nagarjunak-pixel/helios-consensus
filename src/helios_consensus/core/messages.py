"""Wire-level types for the Raft core: log entries, snapshots and messages.

The design follows the "Raft as a library" shape used by etcd/raft: a node never performs
I/O itself. It consumes ``Message`` values and ticks, and emits ``Message`` values plus
entries that the embedding application must persist and apply.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class EntryKind(StrEnum):
    NORMAL = "NORMAL"
    NOOP = "NOOP"
    CONFIG = "CONFIG"


class ConfChangeType(StrEnum):
    ADD_LEARNER = "ADD_LEARNER"
    PROMOTE = "PROMOTE"
    REMOVE = "REMOVE"
    ENTER_JOINT = "ENTER_JOINT"
    LEAVE_JOINT = "LEAVE_JOINT"


@dataclass(frozen=True, slots=True)
class ConfChange:
    """Single-server (§4.1) or Joint Consensus (§6) membership transition."""

    change: ConfChangeType
    node_id: int = 0
    voters: frozenset[int] = frozenset()

    def encode(self) -> str:
        if self.change is ConfChangeType.ENTER_JOINT:
            joined = ",".join(str(v) for v in sorted(self.voters))
            return f"{self.change.value}:{joined}"
        if self.change is ConfChangeType.LEAVE_JOINT:
            return f"{self.change.value}:0"
        return f"{self.change.value}:{self.node_id}"

    @staticmethod
    def decode(raw: str) -> ConfChange:
        kind_raw, _, rest = raw.partition(":")
        kind = ConfChangeType(kind_raw)
        if kind is ConfChangeType.ENTER_JOINT:
            voters = frozenset(int(tok) for tok in rest.split(",") if tok)
            return ConfChange(change=kind, voters=voters)
        if kind is ConfChangeType.LEAVE_JOINT:
            return ConfChange(change=kind)
        return ConfChange(change=kind, node_id=int(rest))


@dataclass(frozen=True, slots=True)
class Entry:
    index: int
    term: int
    kind: EntryKind = EntryKind.NORMAL
    data: str = ""


@dataclass(frozen=True, slots=True)
class Membership:
    """Cluster configuration supporting both simple majorities and Joint Consensus ``C_{old,new}``.

    When ``voters_outgoing`` is non-empty, ``voters`` represents ``C_new`` and
    ``voters_outgoing`` represents ``C_old``: elections, commits and ReadIndex barriers
    require independent majorities from *both* voter sets (Raft paper, section 6).
    """

    voters: frozenset[int]
    learners: frozenset[int] = frozenset()
    voters_outgoing: frozenset[int] = frozenset()

    def __post_init__(self) -> None:
        overlap = (self.voters | self.voters_outgoing) & self.learners
        if overlap:
            raise ValueError(f"nodes {sorted(overlap)} cannot be both voter and learner")

    @property
    def is_joint(self) -> bool:
        return bool(self.voters_outgoing)

    @property
    def all_voters(self) -> frozenset[int]:
        return self.voters | self.voters_outgoing

    @property
    def members(self) -> frozenset[int]:
        return self.voters | self.voters_outgoing | self.learners

    def quorum(self) -> int:
        return len(self.voters) // 2 + 1

    def has_quorum(self, acked: set[int] | frozenset[int]) -> bool:
        in_ok = len(acked & self.voters) >= (len(self.voters) // 2 + 1)
        if not self.voters_outgoing:
            return in_ok
        out_ok = len(acked & self.voters_outgoing) >= (len(self.voters_outgoing) // 2 + 1)
        return in_ok and out_ok

    def tally_votes(self, votes: dict[int, bool]) -> tuple[bool, bool]:
        """Return ``(won, lost)`` across simple or joint voter sets."""

        def _half(group: frozenset[int]) -> tuple[bool, bool]:
            q = len(group) // 2 + 1
            yes = sum(1 for v in group if votes.get(v) is True)
            no = sum(1 for v in group if votes.get(v) is False)
            return yes >= q, no > len(group) - q

        w_in, l_in = _half(self.voters)
        if not self.voters_outgoing:
            return w_in, l_in
        w_out, l_out = _half(self.voters_outgoing)
        return (w_in and w_out), (l_in or l_out)

    def apply(self, cc: ConfChange) -> Membership:
        if cc.change is ConfChangeType.ENTER_JOINT:
            if self.is_joint:
                raise ValueError("already in joint consensus configuration")
            if not cc.voters:
                raise ValueError("joint target voter set cannot be empty")
            return Membership(
                voters=cc.voters,
                learners=self.learners - cc.voters - self.voters,
                voters_outgoing=self.voters,
            )
        if cc.change is ConfChangeType.LEAVE_JOINT:
            if not self.is_joint:
                raise ValueError("not in joint consensus configuration")
            return Membership(voters=self.voters, learners=self.learners)

        if self.is_joint:
            raise ValueError("cannot apply single-server change while in joint consensus")
        voters, learners = set(self.voters), set(self.learners)
        if cc.change is ConfChangeType.ADD_LEARNER:
            if cc.node_id in voters:
                raise ValueError(f"node {cc.node_id} is already a voter")
            learners.add(cc.node_id)
        elif cc.change is ConfChangeType.PROMOTE:
            if cc.node_id not in learners:
                raise ValueError(f"node {cc.node_id} must be a learner before promotion")
            learners.discard(cc.node_id)
            voters.add(cc.node_id)
        else:
            if cc.node_id not in voters and cc.node_id not in learners:
                raise ValueError(f"node {cc.node_id} is not a member")
            voters.discard(cc.node_id)
            learners.discard(cc.node_id)
            if not voters:
                raise ValueError("cannot remove the last voter")
        return Membership(voters=frozenset(voters), learners=frozenset(learners))


@dataclass(frozen=True, slots=True)
class Snapshot:
    index: int
    term: int
    membership: Membership
    data: str = ""


@dataclass(frozen=True, slots=True)
class HardState:
    """State that must reach stable storage before any message is sent."""

    term: int = 0
    vote: int | None = None
    commit: int = 0


class MsgType(StrEnum):
    PRE_VOTE = "PRE_VOTE"
    PRE_VOTE_RESP = "PRE_VOTE_RESP"
    VOTE = "VOTE"
    VOTE_RESP = "VOTE_RESP"
    APPEND = "APPEND"
    APPEND_RESP = "APPEND_RESP"
    HEARTBEAT = "HEARTBEAT"
    HEARTBEAT_RESP = "HEARTBEAT_RESP"
    SNAPSHOT = "SNAPSHOT"
    TIMEOUT_NOW = "TIMEOUT_NOW"


VOTE_REQUESTS = frozenset({MsgType.PRE_VOTE, MsgType.VOTE})
LEADER_MESSAGES = frozenset({MsgType.APPEND, MsgType.HEARTBEAT, MsgType.SNAPSHOT})

# ``context`` value on VOTE requests produced by leadership transfer: bypasses leader stickiness.
CAMPAIGN_TRANSFER = -1


@dataclass(frozen=True, slots=True)
class Message:
    """A single Raft RPC.

    Field meaning depends on ``type`` (mirrors etcd's raftpb.Message):

    * APPEND: ``index``/``log_term`` identify the entry preceding ``entries``.
    * APPEND_RESP: ``index`` is the follower's last matching index on success, or the
      rejected ``prev`` index on failure, with ``reject_hint``/``log_term`` carrying the
      follower's conflict hint for fast log back-tracking.
    * (PRE_)VOTE: ``index``/``log_term`` are the candidate's last log position.
    * HEARTBEAT(_RESP): ``context`` carries a ReadIndex request id (0 means none).
    """

    type: MsgType
    src: int
    dst: int
    term: int
    log_term: int = 0
    index: int = 0
    entries: tuple[Entry, ...] = ()
    commit: int = 0
    reject: bool = False
    reject_hint: int = 0
    snapshot: Snapshot | None = None
    context: int = 0


@dataclass(frozen=True, slots=True)
class ReadState:
    """A linearizable read is safe once the state machine has applied ``index``."""

    request_id: int
    index: int


@dataclass(slots=True)
class Ready:
    """Work the embedding application must perform, in order:

    1. persist ``hard_state``, ``snapshot`` and ``entries`` to stable storage;
    2. send ``messages``;
    3. apply ``snapshot`` then ``committed_entries`` to the state machine;
    4. serve ``read_states`` once applied, then call ``RaftNode.advance``.
    """

    hard_state: HardState | None = None
    entries: list[Entry] = field(default_factory=list)
    snapshot: Snapshot | None = None
    committed_entries: list[Entry] = field(default_factory=list)
    messages: list[Message] = field(default_factory=list)
    read_states: list[ReadState] = field(default_factory=list)

    def is_empty(self) -> bool:
        return (
            self.hard_state is None
            and not self.entries
            and self.snapshot is None
            and not self.committed_entries
            and not self.messages
            and not self.read_states
        )
