"""Global ("god's-eye") safety invariants from the Raft paper, checked during simulation.

* Election Safety: at most one leader can be elected in a given term.
* Log Matching: if two logs contain an entry with the same index and term, the logs are
  identical in all entries up through that index.
* Leader Completeness: a newly elected leader's log contains every committed entry.
* State Machine Safety: no two servers apply different entries at the same index.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from helios_consensus.core.messages import Entry
from helios_consensus.core.node import RaftNode, StateRole


@dataclass(frozen=True, slots=True)
class Violation:
    invariant: str
    tick: int
    detail: str


@dataclass(slots=True)
class InvariantObserver:
    leaders_by_term: dict[int, int] = field(default_factory=dict)
    applied: dict[int, Entry] = field(default_factory=dict)
    last_applied: dict[tuple[int, int], int] = field(default_factory=dict)
    violations: list[Violation] = field(default_factory=list)
    checks: int = 0

    def _fail(self, invariant: str, tick: int, detail: str) -> None:
        self.violations.append(Violation(invariant=invariant, tick=tick, detail=detail))

    # ----------------------------------------------------------------------------- events
    def on_leader(self, node: RaftNode, tick: int) -> None:
        self.checks += 1
        owner = self.leaders_by_term.setdefault(node.term, node.id)
        if owner != node.id:
            self._fail(
                "ElectionSafety",
                tick,
                f"term {node.term} has two leaders: n{owner} and n{node.id}",
            )
        snap_index = node.log.snapshot.index
        for index, entry in self.applied.items():
            if index <= snap_index:
                continue
            if node.log.term(index) != entry.term:
                self._fail(
                    "LeaderCompleteness",
                    tick,
                    f"leader n{node.id} (term {node.term}) is missing committed entry "
                    f"{index}@{entry.term}",
                )
                return

    def on_apply(self, node_id: int, incarnation: int, entry: Entry, tick: int) -> None:
        self.checks += 1
        key = (node_id, incarnation)
        prev = self.last_applied.get(key)
        if prev is not None and entry.index != prev + 1:
            self._fail(
                "StateMachineSafety",
                tick,
                f"n{node_id} applied {entry.index} after {prev} (gap or reorder)",
            )
        self.last_applied[key] = entry.index
        known = self.applied.setdefault(entry.index, entry)
        if known != entry:
            self._fail(
                "StateMachineSafety",
                tick,
                f"index {entry.index}: n{node_id} applied {entry.term}/{entry.data!r} but "
                f"another node applied {known.term}/{known.data!r}",
            )

    def on_snapshot_applied(self, node_id: int, incarnation: int, index: int) -> None:
        self.last_applied[(node_id, incarnation)] = index

    def check_log_matching(self, nodes: list[RaftNode], tick: int) -> None:
        for i, a in enumerate(nodes):
            for b in nodes[i + 1 :]:
                self.checks += 1
                lo = max(a.log.first_index, b.log.first_index)
                hi = min(a.log.last_index, b.log.last_index)
                agree = 0
                for idx in range(hi, lo - 1, -1):
                    if a.log.term(idx) == b.log.term(idx):
                        agree = idx
                        break
                for idx in range(lo, agree + 1):
                    if a.log.entry(idx) != b.log.entry(idx):
                        self._fail(
                            "LogMatching",
                            tick,
                            f"n{a.id} and n{b.id} agree on term at {agree} but differ at {idx}",
                        )
                        return

    def check_roles(self, nodes: list[RaftNode], tick: int) -> None:
        for n in nodes:
            if n.role is StateRole.LEADER and self.leaders_by_term.get(n.term) != n.id:
                self.on_leader(n, tick)
