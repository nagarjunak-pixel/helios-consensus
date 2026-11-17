"""Leader-side replication progress for each follower (etcd-style flow control)."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum


class ProgressState(StrEnum):
    PROBE = "PROBE"  # unknown match point: send one append at a time
    REPLICATE = "REPLICATE"  # steady state: pipeline appends optimistically
    SNAPSHOT = "SNAPSHOT"  # follower needs a snapshot; appends paused


@dataclass(slots=True)
class Progress:
    match: int = 0
    next: int = 1
    state: ProgressState = ProgressState.PROBE
    paused: bool = False
    recent_active: bool = False
    pending_snapshot: int = 0
    snapshot_wait: int = 0
    inflight: list[int] = field(default_factory=list)

    def become_probe(self) -> None:
        if self.state is ProgressState.SNAPSHOT:
            self.next = max(self.match + 1, self.pending_snapshot + 1)
        else:
            self.next = self.match + 1
        self.state = ProgressState.PROBE
        self.pending_snapshot = 0
        self.snapshot_wait = 0
        self.paused = False
        self.inflight.clear()

    def become_replicate(self) -> None:
        self.state = ProgressState.REPLICATE
        self.next = self.match + 1
        self.pending_snapshot = 0
        self.paused = False
        self.inflight.clear()

    def become_snapshot(self, snapshot_index: int) -> None:
        self.state = ProgressState.SNAPSHOT
        self.pending_snapshot = snapshot_index
        self.snapshot_wait = 0
        self.inflight.clear()

    def maybe_update(self, index: int) -> bool:
        """Record a successful append acknowledgement up to ``index``."""
        updated = False
        if index > self.match:
            self.match = index
            updated = True
            self.paused = False
        self.next = max(self.next, index + 1)
        self.inflight = [i for i in self.inflight if i > index]
        return updated

    def maybe_decrease(self, rejected: int, hint: int) -> bool:
        """Back off ``next`` after a rejection. Returns False for stale rejections."""
        if self.state is ProgressState.REPLICATE:
            if rejected <= self.match:
                return False
            self.next = self.match + 1
            return True
        if self.next - 1 != rejected:
            return False  # rejection for an append we no longer care about
        self.next = max(min(rejected, hint + 1), self.match + 1, 1)
        self.paused = False
        return True

    def is_paused(self, max_inflight: int) -> bool:
        if self.state is ProgressState.PROBE:
            return self.paused
        if self.state is ProgressState.REPLICATE:
            return len(self.inflight) >= max_inflight
        return True

    def sent_entries(self, last: int) -> None:
        if self.state is ProgressState.REPLICATE:
            self.next = last + 1
            self.inflight.append(last)
        elif self.state is ProgressState.PROBE:
            self.paused = True
