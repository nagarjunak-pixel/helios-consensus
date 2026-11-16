"""TLA+ / Lamport-Style Explicit-State Breadth-First Model Checker (`BFS` State-Space Explorer).

Exhaustively explores reachable asynchronous 3-node consensus configurations `(terms, roles, logs,
committed)` up to bounded term/log horizon, checking:
  1) `ElectionSafety`: at most one leader per term
  2) `LogMatching`: matching `(index, term)` implies identical prefix
  3) `LeaderCompleteness`: every committed entry appears in every higher-term leader's log!

Also runs against a deliberate **Mutant Buggy Protocol** (`enforce_up_to_date_vote_check=False`)
to automatically synthesize the minimal shortest-path counterexample trace!
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass

from helios_consensus.types import ModelCheckerReport

# Abstract State of a 3-node asynchronous consensus model:
#   terms: tuple[int, int, int]
#   leaders: frozenset[tuple[int, int]]  # pairs of (term, leader_node_id)
#   logs: tuple[tuple[int, ...], ...]    # per-node tuple of entry terms
#   committed: frozenset[tuple[int, int]]  # committed (index, term) pairs


@dataclass(frozen=True)
class AbstractConsensusState:
    terms: tuple[int, int, int]
    leaders: frozenset[tuple[int, int]]
    logs: tuple[tuple[int, ...], tuple[int, ...], tuple[int, ...]]
    committed: frozenset[tuple[int, int]]


def _log_up_to_date(cand_log: tuple[int, ...], voter_log: tuple[int, ...]) -> bool:
    c_last = cand_log[-1] if cand_log else 0
    v_last = voter_log[-1] if voter_log else 0
    return (c_last, len(cand_log)) >= (v_last, len(voter_log))


def _successors(
    state: AbstractConsensusState,
    *,
    max_term: int,
    max_log_len: int,
    enforce_up_to_date_vote_check: bool,
) -> list[tuple[str, AbstractConsensusState]]:
    out: list[tuple[str, AbstractConsensusState]] = []

    # Action 1: Node `c` starts election in `new_term` and gathers a 2-of-3 quorum `{c, v}`
    for c in range(3):
        new_term = state.terms[c] + 1
        if new_term > max_term:
            continue
        # Cannot elect a second leader if a quorum already voted in `new_term`
        for v in range(3):
            if v == c:
                continue
            if state.terms[v] >= new_term:
                continue
            if enforce_up_to_date_vote_check and not _log_up_to_date(state.logs[c], state.logs[v]):
                continue
            next_terms = list(state.terms)
            next_terms[c] = new_term
            next_terms[v] = new_term
            next_leaders = set(state.leaders)
            next_leaders.add((new_term, c))
            out.append(
                (
                    f"Elect(n{c}, term={new_term}, quorum={{n{c},n{v}}})",
                    AbstractConsensusState(
                        terms=tuple(next_terms),  # type: ignore[arg-type]
                        leaders=frozenset(next_leaders),
                        logs=state.logs,
                        committed=state.committed,
                    ),
                )
            )

    # Action 2: Active leader `(t, ldr)` appends an entry in term `t` (if ldr still in term `t`)
    for t, ldr in state.leaders:
        if state.terms[ldr] != t or len(state.logs[ldr]) >= max_log_len:
            continue
        new_logs = [list(lg) for lg in state.logs]
        new_logs[ldr].append(t)
        out.append(
            (
                f"ClientAppend(n{ldr}, term={t}, idx={len(new_logs[ldr]) - 1})",
                AbstractConsensusState(
                    terms=state.terms,
                    leaders=state.leaders,
                    logs=(tuple(new_logs[0]), tuple(new_logs[1]), tuple(new_logs[2])),
                    committed=state.committed,
                ),
            )
        )

    # Action 3: Leader `(t, ldr)` replicates its full prefix log to follower `f` (with term <= t)
    for t, ldr in state.leaders:
        if state.terms[ldr] != t:
            continue
        for f in range(3):
            if f == ldr or state.terms[f] > t:
                continue
            if state.logs[f] == state.logs[ldr] and state.terms[f] == t:
                continue
            new_terms = list(state.terms)
            new_terms[f] = t
            new_logs = [list(lg) for lg in state.logs]
            new_logs[f] = list(state.logs[ldr])
            # Check if any current-term entry of `ldr` now sits on >= 2 nodes -> commit!
            new_comm = set(state.committed)
            for idx, entry_term in enumerate(new_logs[ldr]):
                if entry_term == t:
                    cnt = sum(
                        1
                        for node_lg in new_logs
                        if idx < len(node_lg) and node_lg[idx] == entry_term
                    )
                    if cnt >= 2:
                        # Commit entire prefix 0..idx!
                        for k in range(idx + 1):
                            new_comm.add((k, new_logs[ldr][k]))
            out.append(
                (
                    f"ReplicateAndCommit(n{ldr}->n{f}, term={t})",
                    AbstractConsensusState(
                        terms=tuple(new_terms),  # type: ignore[arg-type]
                        leaders=state.leaders,
                        logs=(tuple(new_logs[0]), tuple(new_logs[1]), tuple(new_logs[2])),
                        committed=frozenset(new_comm),
                    ),
                )
            )

    return out


def _check_invariants(state: AbstractConsensusState) -> tuple[bool, bool, bool]:
    # 1. ElectionSafety: at most 1 leader per term
    seen: dict[int, int] = {}
    for t, ldr in state.leaders:
        if t in seen and seen[t] != ldr:
            return False, True, True
        seen[t] = ldr

    # 2. LogMatching: if two logs share (idx, term), prefixes 0..idx must match
    for i in range(3):
        for j in range(i + 1, 3):
            li, lj = state.logs[i], state.logs[j]
            for k in range(min(len(li), len(lj))):
                if li[k] == lj[k] and li[: k + 1] != lj[: k + 1]:
                    return True, False, True

    # 3. LeaderCompleteness: every committed (idx, c_term) must exist in every leader of term > c_term!
    for c_idx, c_term in state.committed:
        for l_term, ldr in state.leaders:
            if l_term > c_term:
                ldr_log = state.logs[ldr]
                if c_idx >= len(ldr_log) or ldr_log[c_idx] != c_term:
                    return True, True, False

    return True, True, True


def _run_bfs(
    *,
    max_term: int,
    max_log_len: int,
    enforce_up_to_date_vote_check: bool,
) -> tuple[int, int, int, bool, bool, bool, list[str]]:
    init = AbstractConsensusState(
        terms=(0, 0, 0),
        leaders=frozenset(),
        logs=((), (), ()),
        committed=frozenset(),
    )
    visited: set[AbstractConsensusState] = {init}
    queue: deque[tuple[AbstractConsensusState, list[str]]] = deque([(init, [])])
    transitions = 0
    max_depth = 0

    while queue:
        curr, trace = queue.popleft()
        max_depth = max(max_depth, len(trace))
        e_ok, m_ok, c_ok = _check_invariants(curr)
        if not (e_ok and m_ok and c_ok):
            return len(visited), transitions, max_depth, e_ok, m_ok, c_ok, trace

        for action_label, nxt in _successors(
            curr,
            max_term=max_term,
            max_log_len=max_log_len,
            enforce_up_to_date_vote_check=enforce_up_to_date_vote_check,
        ):
            transitions += 1
            if nxt not in visited:
                visited.add(nxt)
                queue.append((nxt, [*trace, action_label]))

    return len(visited), transitions, max_depth, True, True, True, []


def explore_consensus_state_space(
    *,
    max_term: int = 2,
    max_log_len: int = 2,
) -> ModelCheckerReport:
    """Exhaustively model-check correct Raft safety invariants across all reachable states AND
    prove counterexample synthesis on a mutant protocol lacking the §5.4.1 log-freshness check.
    """
    states, trans, depth, e_ok, m_ok, c_ok, _ = _run_bfs(
        max_term=max_term,
        max_log_len=max_log_len,
        enforce_up_to_date_vote_check=True,
    )
    _, _, _, _, _, mutant_c_ok, counterexample = _run_bfs(
        max_term=max_term,
        max_log_len=max_log_len,
        enforce_up_to_date_vote_check=False,
    )

    return ModelCheckerReport(
        states_explored=states,
        transitions_checked=trans,
        max_bfs_depth=depth,
        election_safety_holds=e_ok,
        log_matching_holds=m_ok,
        leader_completeness_holds=c_ok,
        mutant_bug_caught=not mutant_c_ok,
        mutant_counterexample_steps=counterexample,
    )
