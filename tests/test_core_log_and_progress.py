"""Comprehensive unit tests for RaftLog (including Ongaro Figure 7 logs) and Progress flow control."""

from __future__ import annotations

import pytest

from helios_consensus.core.log import LogSafetyError, RaftLog
from helios_consensus.core.messages import Entry, EntryKind, Membership, Snapshot
from helios_consensus.core.progress import Progress, ProgressState


def _mk_entries(terms: list[int], start_index: int = 1) -> list[Entry]:
    return [
        Entry(index=start_index + off, term=t, kind=EntryKind.NORMAL, data=f"c{start_index + off}")
        for off, t in enumerate(terms)
    ]


def test_raft_log_empty_bounds_and_queries() -> None:
    log = RaftLog()
    assert log.first_index == 1
    assert log.last_index == 0
    assert log.last_term() == 0
    assert log.term(0) == 0
    assert log.term(-1) == -1
    assert log.term(5) == -1
    assert log.unstable_entries() == []
    assert log.next_committed_entries() == []

    with pytest.raises(IndexError):
        log.entry(1)
    with pytest.raises(ValueError, match="non-contiguous"):
        log.append([Entry(index=2, term=1)])


@pytest.mark.parametrize(
    ("name", "f_terms"),
    [
        ("7a_missing_tail", [1, 1, 1, 4, 4, 5, 5, 6, 6]),
        ("7b_missing_many", [1, 1, 1, 4]),
        ("7c_extra_uncommitted_t6", [1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6]),
        ("7d_extra_higher_term_t7", [1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7]),
        ("7e_diverged_early_t4", [1, 1, 1, 4, 4, 4, 4]),
        ("7f_complex_multi_term", [1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3]),
    ],
)
def test_raft_log_ongaro_figure_7_follower_scenarios(name: str, f_terms: list[int]) -> None:
    """Test all 6 follower log divergence shapes from Figure 7 of the Raft paper.

    Leader at term 8 has log terms: [1, 1, 1, 4, 4, 5, 5, 6, 6, 6] (indices 1..10).
    """
    leader_terms = [1, 1, 1, 4, 4, 5, 5, 6, 6, 6]
    leader_entries = tuple(_mk_entries(leader_terms))

    f_log = RaftLog()
    f_log.append(_mk_entries(f_terms))
    # Simulate leader probing backwards using find_conflict_by_term until match
    probe_prev = len(leader_terms)
    while probe_prev >= 0:
        prev_term = 0 if probe_prev == 0 else leader_terms[probe_prev - 1]
        suffix = leader_entries[probe_prev:]
        res = f_log.maybe_append(probe_prev, prev_term, commit=10, entries=suffix)
        if res is not None:
            break
        hint = f_log.find_conflict_by_term(min(probe_prev, f_log.last_index), prev_term)
        probe_prev = min(probe_prev - 1, hint)

    assert f_log.committed == 10, f"failed on {name}"
    next_leader_entry = (Entry(index=11, term=8, data="c11"),)
    res2 = f_log.maybe_append(10, 6, commit=11, entries=next_leader_entry)
    assert res2 == 11
    assert [f_log.term(i) for i in range(1, 12)] == [*leader_terms, 8], f"mismatch on {name}"


def test_raft_log_safety_errors_compaction_and_restore() -> None:
    voters = Membership(voters=frozenset({1, 2, 3}))
    log = RaftLog(Snapshot(index=0, term=0, membership=voters))
    log.append(_mk_entries([1, 1, 2, 2, 3]))
    log.stable_to(5)
    assert log.unstable_entries() == []
    log.commit_to(4)
    assert [e.index for e in log.next_committed_entries()] == [1, 2, 3, 4]
    log.applied_to(4)
    assert log.next_committed_entries() == []

    # Conflicting entry at committed index 3 must raise LogSafetyError
    with pytest.raises(LogSafetyError, match="conflicts with committed"):
        log.maybe_append(2, 1, commit=4, entries=(Entry(index=3, term=9, data="bad"),))

    with pytest.raises(LogSafetyError, match="beyond last index"):
        log.commit_to(99)

    with pytest.raises(LogSafetyError, match="beyond commit"):
        log.applied_to(5)

    # Compaction checks
    with pytest.raises(LogSafetyError, match="beyond applied"):
        log.compact(Snapshot(index=5, term=3, membership=voters))
    with pytest.raises(LogSafetyError, match="snapshot term"):
        log.compact(Snapshot(index=3, term=99, membership=voters))

    snap3 = Snapshot(index=3, term=2, membership=voters, data="s3")
    log.compact(snap3)
    assert log.first_index == 4
    assert log.term(3) == 2
    assert log.term(2) == -1
    # Compacting an older snapshot is a safe no-op
    log.compact(Snapshot(index=2, term=1, membership=voters))
    assert log.snapshot.index == 3

    with pytest.raises(IndexError, match="compacted"):
        log.slice(2, 5)

    # Restore from leader snapshot
    with pytest.raises(LogSafetyError, match="older than the commit"):
        log.restore(Snapshot(index=4, term=2, membership=voters))
    snap10 = Snapshot(index=10, term=4, membership=voters, data="s10")
    log.restore(snap10)
    assert log.committed == 10 and log.last_index == 10 and log.pending_snapshot == snap10

    # Non-contiguous load from storage must fail loudly
    bad_log = RaftLog()
    with pytest.raises(LogSafetyError, match="non-contiguous"):
        bad_log.load([Entry(index=2, term=1)], commit=0)


def test_raft_log_election_up_to_date_ordering() -> None:
    log = RaftLog()
    log.append(_mk_entries([1, 2, 2]))  # last_index=3, last_term=2
    assert log.is_up_to_date(1, 3) is True  # higher term wins even if shorter
    assert log.is_up_to_date(3, 2) is True  # equal term and length
    assert log.is_up_to_date(4, 2) is True  # equal term, longer
    assert log.is_up_to_date(2, 2) is False  # equal term, shorter
    assert log.is_up_to_date(10, 1) is False  # lower term loses even if much longer


def test_progress_flow_control_transitions() -> None:
    pr = Progress(match=0, next=5)
    assert pr.state is ProgressState.PROBE
    assert not pr.is_paused(max_inflight=2)
    pr.sent_entries(5)
    assert pr.paused is True and pr.is_paused(max_inflight=2)

    # Stale rejection ignored when rejected != next - 1 in PROBE
    assert pr.maybe_decrease(rejected=9, hint=3) is False
    assert pr.maybe_decrease(rejected=4, hint=2) is True
    assert pr.next == 3 and not pr.paused

    # Update advances match and lets leader switch to REPLICATE
    assert pr.maybe_update(3) is True
    assert pr.maybe_update(2) is False
    pr.become_replicate()
    assert pr.state is ProgressState.REPLICATE and pr.next == 4

    pr.sent_entries(5)
    pr.sent_entries(7)
    assert pr.is_paused(max_inflight=2) is True
    assert pr.maybe_update(5) is True
    assert pr.inflight == [7] and not pr.is_paused(max_inflight=2)

    # Rejection in REPLICATE drops next back to match + 1; stale <= match ignored
    assert pr.maybe_decrease(rejected=5, hint=4) is False
    assert pr.maybe_decrease(rejected=7, hint=5) is True
    assert pr.next == 6

    # Snapshot transition and recovery to PROBE
    pr.become_snapshot(20)
    assert pr.is_paused(max_inflight=4) is True
    pr.become_probe()
    assert pr.state is ProgressState.PROBE and pr.next == 21
