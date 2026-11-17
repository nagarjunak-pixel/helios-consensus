"""Replicated key-value state machine with exactly-once client sessions."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from helios_consensus.core.messages import Entry, EntryKind


class KVOp(StrEnum):
    PUT = "put"
    CAS = "cas"


@dataclass(frozen=True, slots=True)
class KVCommand:
    op: KVOp
    key: str
    value: int
    client: int
    seq: int
    expected: int | None = None

    def encode(self) -> str:
        return json.dumps(
            {
                "op": self.op.value,
                "k": self.key,
                "v": self.value,
                "e": self.expected,
                "c": self.client,
                "s": self.seq,
            },
            separators=(",", ":"),
        )

    @staticmethod
    def decode(raw: str) -> KVCommand:
        d = json.loads(raw)
        return KVCommand(
            op=KVOp(d["op"]),
            key=str(d["k"]),
            value=int(d["v"]),
            expected=d["e"],
            client=int(d["c"]),
            seq=int(d["s"]),
        )


@dataclass(frozen=True, slots=True)
class ApplyResult:
    client: int
    seq: int
    result: bool
    duplicate: bool


class KVStateMachine:
    """Deterministic register map.

    Sessions remember the last ``(seq, result)`` per client so a retried command that was
    already applied returns the cached result instead of executing twice.
    """

    def __init__(self) -> None:
        self.data: dict[str, int] = {}
        self.sessions: dict[int, tuple[int, bool]] = {}
        self.applied_index = 0

    def apply(self, entry: Entry) -> ApplyResult | None:
        if entry.index <= self.applied_index:
            raise ValueError(f"entry {entry.index} already applied (at {self.applied_index})")
        self.applied_index = entry.index
        if entry.kind is not EntryKind.NORMAL:
            return None
        cmd = KVCommand.decode(entry.data)
        session = self.sessions.get(cmd.client)
        if session is not None and session[0] >= cmd.seq:
            return ApplyResult(cmd.client, cmd.seq, session[1], duplicate=True)
        if cmd.op is KVOp.PUT or self.data.get(cmd.key) == cmd.expected:
            self.data[cmd.key] = cmd.value
            result = True
        else:
            result = False
        self.sessions[cmd.client] = (cmd.seq, result)
        return ApplyResult(cmd.client, cmd.seq, result, duplicate=False)

    def read(self, key: str) -> int | None:
        return self.data.get(key)

    def snapshot(self) -> str:
        return json.dumps(
            {
                "data": self.data,
                "sessions": {str(c): list(v) for c, v in self.sessions.items()},
                "applied": self.applied_index,
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    @staticmethod
    def restore(raw: str) -> KVStateMachine:
        sm = KVStateMachine()
        if not raw:
            return sm
        d: dict[str, Any] = json.loads(raw)
        sm.data = {str(k): int(v) for k, v in d["data"].items()}
        sm.sessions = {int(c): (int(v[0]), bool(v[1])) for c, v in d["sessions"].items()}
        sm.applied_index = int(d["applied"])
        return sm

    def digest(self) -> str:
        return hashlib.sha256(json.dumps(self.data, sort_keys=True).encode()).hexdigest()[:16]
