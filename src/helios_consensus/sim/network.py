"""Deterministic simulated network: seeded delays, loss, duplication, reordering, partitions."""

from __future__ import annotations

import heapq
import random
from dataclasses import dataclass, field

from helios_consensus.core.messages import Message


@dataclass(slots=True)
class NetworkStats:
    sent: int = 0
    delivered: int = 0
    dropped_partition: int = 0
    dropped_loss: int = 0
    dropped_dead: int = 0
    duplicated: int = 0


@dataclass(slots=True)
class SimNetwork:
    rng: random.Random
    min_delay: int = 1
    max_delay: int = 4
    drop_rate: float = 0.0
    dup_rate: float = 0.0
    blocked: set[tuple[int, int]] = field(default_factory=set)
    stats: NetworkStats = field(default_factory=NetworkStats)
    _queue: list[tuple[int, int, Message]] = field(default_factory=list)
    _seq: int = 0

    def send(self, m: Message, now: int) -> None:
        self.stats.sent += 1
        if (m.src, m.dst) in self.blocked:
            self.stats.dropped_partition += 1
            return
        if self.drop_rate and self.rng.random() < self.drop_rate:
            self.stats.dropped_loss += 1
            return
        self._push(now + self.rng.randint(self.min_delay, self.max_delay), m)
        if self.dup_rate and self.rng.random() < self.dup_rate:
            self.stats.duplicated += 1
            self._push(now + self.rng.randint(self.min_delay, self.max_delay * 2), m)

    def _push(self, at: int, m: Message) -> None:
        self._seq += 1
        heapq.heappush(self._queue, (at, self._seq, m))

    def defer(self, m: Message, at: int) -> None:
        self._push(at, m)

    def due(self, now: int) -> list[Message]:
        out: list[Message] = []
        while self._queue and self._queue[0][0] <= now:
            _, _, m = heapq.heappop(self._queue)
            if (m.src, m.dst) in self.blocked:
                self.stats.dropped_partition += 1
                continue
            out.append(m)
        return out

    def in_flight(self) -> int:
        return len(self._queue)

    # ----------------------------------------------------------------------------- partitions
    def isolate(self, group_a: set[int], group_b: set[int]) -> None:
        self.blocked = {(a, b) for a in group_a for b in group_b} | {
            (b, a) for a in group_a for b in group_b
        }

    def bridge(self, left: set[int], right: set[int], bridge_node: int) -> None:
        """Two halves that only communicate through ``bridge_node`` (Jepsen 'bridge')."""
        self.isolate(left, right)
        self.blocked = {link for link in self.blocked if bridge_node not in link}

    def heal(self) -> None:
        self.blocked = set()
