"""In-memory Raft log with snapshot base, unstable tail tracking and conflict resolution."""

from __future__ import annotations

from helios_consensus.core.messages import Entry, Membership, Snapshot


class LogSafetyError(AssertionError):
    """Raised when an operation would overwrite or contradict committed state."""


class RaftLog:
    """Ordered log ``(snapshot.index, last_index]`` plus the snapshot that precedes it.

    ``stable_index`` tracks how much of the tail has been handed to stable storage, so the
    node can report exactly the entries that still need persisting.
    """

    def __init__(self, snapshot: Snapshot | None = None) -> None:
        self.snapshot: Snapshot = snapshot or Snapshot(
            index=0, term=0, membership=Membership(voters=frozenset())
        )
        self._entries: list[Entry] = []
        self.committed = self.snapshot.index
        self.applied = self.snapshot.index
        self.stable_index = self.snapshot.index
        self.pending_snapshot: Snapshot | None = None
        self.allow_conflict_without_truncate = False  # mutation-testing hook

    # ----------------------------------------------------------------------------- queries
    @property
    def first_index(self) -> int:
        return self.snapshot.index + 1

    @property
    def last_index(self) -> int:
        return self.snapshot.index + len(self._entries)

    def last_term(self) -> int:
        return self.term(self.last_index)

    def term(self, index: int) -> int:
        """Term of ``index``; ``-1`` when unknown (compacted away or beyond the tail)."""
        if index == self.snapshot.index:
            return self.snapshot.term
        if index < self.snapshot.index or index > self.last_index:
            return -1
        return self._entries[index - self.first_index].term

    def entry(self, index: int) -> Entry:
        if index < self.first_index or index > self.last_index:
            raise IndexError(f"entry {index} unavailable (first={self.first_index})")
        return self._entries[index - self.first_index]

    def slice(self, lo: int, hi: int) -> list[Entry]:
        """Entries in ``[lo, hi)``."""
        if lo < self.first_index:
            raise IndexError(f"entry {lo} compacted (first={self.first_index})")
        hi = min(hi, self.last_index + 1)
        if lo >= hi:
            return []
        return self._entries[lo - self.first_index : hi - self.first_index]

    def match_term(self, index: int, term: int) -> bool:
        return self.term(index) == term

    def is_up_to_date(self, last_index: int, last_term: int) -> bool:
        """Election restriction (Raft paper, section 5.4.1)."""
        return (last_term, last_index) >= (self.last_term(), self.last_index)

    def find_conflict_by_term(self, index: int, term: int) -> int:
        """Largest ``i <= index`` whose term is ``<= term`` (fast back-tracking hint)."""
        index = min(index, self.last_index)
        while index > self.snapshot.index:
            t = self.term(index)
            if t <= term:
                return index
            index -= 1
        return self.snapshot.index

    def unstable_entries(self) -> list[Entry]:
        if self.stable_index >= self.last_index:
            return []
        return self.slice(max(self.stable_index + 1, self.first_index), self.last_index + 1)

    def next_committed_entries(self) -> list[Entry]:
        lo = max(self.applied + 1, self.first_index)
        if lo > self.committed:
            return []
        return self.slice(lo, self.committed + 1)

    # ----------------------------------------------------------------------------- mutation
    def append(self, entries: list[Entry] | tuple[Entry, ...]) -> int:
        """Leader-side append of fresh entries at the tail."""
        for e in entries:
            if e.index != self.last_index + 1:
                raise ValueError(
                    f"non-contiguous append: got {e.index}, want {self.last_index + 1}"
                )
            self._entries.append(e)
        return self.last_index

    def maybe_append(
        self, prev_index: int, prev_term: int, commit: int, entries: tuple[Entry, ...]
    ) -> int | None:
        """Follower-side append. Returns the last index known to match the leader, or None."""
        if not self.match_term(prev_index, prev_term):
            return None
        last_new = prev_index + len(entries)
        conflict = self._find_conflict(entries)
        if conflict:
            if conflict <= self.committed:
                raise LogSafetyError(
                    f"entry {conflict} conflicts with committed index {self.committed}"
                )
            offset = conflict - (prev_index + 1)
            if not self.allow_conflict_without_truncate:
                self._truncate_from(conflict)
                self.append(entries[offset:])
            else:
                # Deliberately broken variant: keep stale suffix, overwrite in place only.
                for e in entries[offset:]:
                    if e.index <= self.last_index:
                        self._entries[e.index - self.first_index] = e
                    else:
                        self._entries.append(e)
        self.commit_to(min(commit, last_new))
        return last_new

    def _find_conflict(self, entries: tuple[Entry, ...]) -> int:
        for e in entries:
            if e.index < self.first_index:
                continue
            if not self.match_term(e.index, e.term):
                return e.index
        return 0

    def _truncate_from(self, index: int) -> None:
        if index <= self.last_index:
            del self._entries[index - self.first_index :]
            self.stable_index = min(self.stable_index, index - 1)

    def commit_to(self, index: int) -> bool:
        if index > self.committed:
            if index > self.last_index:
                raise LogSafetyError(f"commit {index} beyond last index {self.last_index}")
            self.committed = index
            return True
        return False

    def stable_to(self, index: int) -> None:
        self.stable_index = max(self.stable_index, min(index, self.last_index))

    def applied_to(self, index: int) -> None:
        if index > self.committed:
            raise LogSafetyError(f"apply {index} beyond commit {self.committed}")
        self.applied = max(self.applied, index)

    def compact(self, snapshot: Snapshot) -> None:
        """Discard entries covered by a locally produced snapshot."""
        if snapshot.index <= self.snapshot.index:
            return
        if snapshot.index > self.applied:
            raise LogSafetyError(
                f"cannot compact to {snapshot.index} beyond applied {self.applied}"
            )
        if self.term(snapshot.index) != snapshot.term:
            raise LogSafetyError("snapshot term does not match log")
        del self._entries[: snapshot.index - self.snapshot.index]
        self.snapshot = snapshot
        self.stable_index = max(self.stable_index, snapshot.index)

    def restore(self, snapshot: Snapshot) -> None:
        """Replace the log with a snapshot received from the leader."""
        if snapshot.index <= self.committed:
            raise LogSafetyError("restoring a snapshot older than the commit index")
        self._entries = []
        self.snapshot = snapshot
        self.committed = snapshot.index
        self.stable_index = snapshot.index
        self.pending_snapshot = snapshot

    def load(self, entries: list[Entry], commit: int) -> None:
        """Rebuild from stable storage after a restart."""
        self._entries = [e for e in entries if e.index > self.snapshot.index]
        for i, e in enumerate(self._entries):
            if e.index != self.first_index + i:
                raise LogSafetyError("stable storage returned a non-contiguous log")
        self.stable_index = self.last_index
        self.committed = max(self.snapshot.index, min(commit, self.last_index))
        self.applied = self.snapshot.index
