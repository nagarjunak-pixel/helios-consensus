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


@dataclass(frozen=True, slots=True)
class ConfChange:
    """A single-server membership change (Ongaro dissertation, section 4.1)."""

    change: ConfChangeType
    node_id: int

    def encode(self) -> str:
        return f"{self.change.value}:{self.node_id}"

    @staticmethod
    def decode(raw: str) -> ConfChange:
        kind, _, node = raw.partition(":")
        return ConfChange(change=ConfChangeType(kind), node_id=int(node))


@dataclass(frozen=True, slots=True)
class Entry:
    index: int
    term: int
    kind: EntryKind = EntryKind.NORMAL
    data: str = ""


@dataclass(frozen=True, slots=True)
class Membership:
    """Voting members plus non-voting learners."""

    voters: frozenset[int]
    learners: frozenset[int] = frozenset()

    def __post_init__(self) -> None:
        overlap = self.voters & self.learners
        if overlap:
            raise ValueError(f"nodes {sorted(overlap)} cannot be both voter and learner")

    @property
    def members(self) -> frozenset[int]:
        return self.voters | self.learners

    def quorum(self) -> int:
        return len(self.voters) // 2 + 1

    def apply(self, cc: ConfChange) -> Membership:
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
