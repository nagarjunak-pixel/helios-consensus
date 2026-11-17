"""Linearizability checking for register histories (Wing & Gong search with Lowe's memo).

This is the same algorithm used by Porcupine and Knossos' ``wgl`` checker:

* every operation contributes a call event and a return event, sorted by time;
* the search repeatedly picks a pending call, applies it to the model, and "lifts" the
  operation out of the history; reaching a return event means some operation that should
  already have taken effect could not be linearized, so the search back-tracks;
* a cache of ``(linearized-set, model-state)`` prunes equivalent branches.

Indeterminate operations (client timed out, outcome unknown) get an infinite return time:
they may take effect at any point after their invocation, or effectively never.

Histories are partitioned per key (P-compositionality), which keeps each search small.
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from enum import StrEnum


class OpKind(StrEnum):
    READ = "read"
    WRITE = "write"
    CAS = "cas"


@dataclass(frozen=True, slots=True)
class Operation:
    """One client operation on a single register.

    * WRITE: ``value`` is the written value.
    * READ: ``value`` is the observed value (``None`` = key absent).
    * CAS: ``expected`` -> ``value``; ``ok`` is the result (``None`` when unknown).
    ``ret`` is ``None`` for indeterminate operations.
    """

    op_id: int
    process: int
    kind: OpKind
    key: str
    value: int | None
    call: int
    ret: int | None
    expected: int | None = None
    ok: bool | None = None


State = int | None


def step_register(state: State, op: Operation) -> tuple[bool, State]:
    if op.kind is OpKind.WRITE:
        return True, op.value
    if op.kind is OpKind.READ:
        return op.value == state, state
    if op.ok is None:
        return True, (op.value if state == op.expected else state)
    if op.ok:
        return state == op.expected, op.value
    return state != op.expected, state


@dataclass(eq=False, slots=True)
class _Event:
    op: Operation
    is_call: bool
    time: float
    order: int
    match: _Event | None = None
    prev: _Event | None = None
    next: _Event | None = None


@dataclass(slots=True)
class KeyResult:
    key: str
    ok: bool
    operations: int
    states_explored: int
    exhausted_budget: bool = False


@dataclass(slots=True)
class LinearizabilityReport:
    ok: bool
    keys: list[KeyResult] = field(default_factory=list)

    @property
    def operations(self) -> int:
        return sum(k.operations for k in self.keys)

    @property
    def states_explored(self) -> int:
        return sum(k.states_explored for k in self.keys)

    @property
    def failing_keys(self) -> list[str]:
        return [k.key for k in self.keys if not k.ok]


def _build_list(ops: list[Operation]) -> _Event:
    events: list[_Event] = []
    for op in ops:
        call = _Event(op=op, is_call=True, time=float(op.call), order=0)
        ret_time = math.inf if op.ret is None else float(op.ret)
        ret = _Event(op=op, is_call=False, time=ret_time, order=1)
        call.match = ret
        events.extend((call, ret))
    # Calls sort before returns at equal timestamps: treating such operations as
    # concurrent is the conservative (more permissive) interpretation.
    events.sort(key=lambda e: (e.time, e.order, e.op.op_id))
    head = _Event(op=ops[0], is_call=False, time=-math.inf, order=-1)
    prev = head
    for e in events:
        prev.next = e
        e.prev = prev
        prev = e
    return head


def _lift(call: _Event) -> None:
    assert call.prev is not None and call.match is not None
    call.prev.next = call.next
    if call.next is not None:
        call.next.prev = call.prev
    ret = call.match
    assert ret.prev is not None
    ret.prev.next = ret.next
    if ret.next is not None:
        ret.next.prev = ret.prev


def _unlift(call: _Event) -> None:
    ret = call.match
    assert ret is not None and ret.prev is not None and call.prev is not None
    ret.prev.next = ret
    if ret.next is not None:
        ret.next.prev = ret
    call.prev.next = call
    if call.next is not None:
        call.next.prev = call


def python_check_register(
    ops: list[Operation], *, budget: int = 2_000_000
) -> tuple[bool, int, bool]:
    """Pure-Python Wing & Gong + Lowe solver; returns ``(ok, explored, budget_exhausted)``."""
    if not ops:
        return True, 0, False
    index_of = {op.op_id: i for i, op in enumerate(ops)}
    head = _build_list(ops)
    state: State = None
    linearized = 0
    cache: set[tuple[int, State]] = set()
    stack: list[tuple[_Event, State]] = []
    entry = head.next
    explored = 0
    while head.next is not None:
        if explored >= budget:
            return True, explored, True
        assert entry is not None
        if entry.is_call:
            ok, new_state = step_register(state, entry.op)
            if ok:
                bit = 1 << index_of[entry.op.op_id]
                key = (linearized | bit, new_state)
                if key not in cache:
                    explored += 1
                    cache.add(key)
                    stack.append((entry, state))
                    state = new_state
                    linearized |= bit
                    _lift(entry)
                    entry = head.next
                    continue
            entry = entry.next
        else:
            if not stack:
                return False, explored, False
            call, state = stack.pop()
            linearized &= ~(1 << index_of[call.op.op_id])
            _unlift(call)
            entry = call.next
    return True, explored, False


def check_register(
    ops: list[Operation], *, budget: int = 2_000_000, force_python: bool = False
) -> tuple[bool, int, bool]:
    """Returns ``(linearizable, states_explored, budget_exhausted)``."""
    if not ops:
        return True, 0, False
    if not force_python and len(ops) <= 62:
        from helios_consensus.kernels.native_ring import get_helios_bridge

        native = get_helios_bridge().wgl_check_register(ops, budget=budget)
        if native is not None:
            return native
    return python_check_register(ops, budget=budget)


def check_history(
    ops: list[Operation], *, budget_per_key: int = 2_000_000
) -> LinearizabilityReport:
    by_key: dict[str, list[Operation]] = defaultdict(list)
    for op in ops:
        by_key[op.key].append(op)
    report = LinearizabilityReport(ok=True)
    for key in sorted(by_key):
        key_ops = by_key[key]
        ok, explored, exhausted = check_register(key_ops, budget=budget_per_key)
        report.keys.append(
            KeyResult(
                key=key,
                ok=ok,
                operations=len(key_ops),
                states_explored=explored,
                exhausted_budget=exhausted,
            )
        )
        report.ok = report.ok and ok
    return report
