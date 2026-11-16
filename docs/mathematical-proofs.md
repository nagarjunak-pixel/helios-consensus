# Mathematical Proofs & Consensus Invariants in `helios-consensus`

## 1. Raft Election Restriction & Current-Term Commit Rule (Ongaro & Ousterhout §5.4.1–§5.4.2)

Let a cluster of odd size $N = 2k + 1$ require strict majorities $Q = k + 1$.
For any two majorities $Q_1, Q_2 \subseteq \{0, \dots, N-1\}$:

$$|Q_1 \cap Q_2| = |Q_1| + |Q_2| - |Q_1 \cup Q_2| \ge (k + 1) + (k + 1) - (2k + 1) = 1$$

- **Election Safety**: Because each node sets `voted_for` at most once per term $t$, two distinct candidates cannot both assemble $k + 1$ votes in term $t$.
- **Up-to-Date Log Partial Order (§5.4.1)**: Define $(T_a, I_a) \succeq (T_b, I_b) \iff (T_a > T_b) \lor (T_a = T_b \land I_a \ge I_b)$. A voter $v$ grants `RequestVote` to candidate $c$ only if $(T_c, I_c) \succeq (T_v, I_v)$.
- **Leader Completeness Proof**: Suppose entry $e$ at `(idx, t_0)` is committed on quorum $Q_{\text{commit}}$. Any future leader $L$ in term $t > t_0$ must collect votes from an election quorum $Q_{\text{elect}}$, which intersects $Q_{\text{commit}}$ in at least one voter $v^* \in Q_{\text{commit}} \cap Q_{\text{elect}}$. Because $v^*$ holds $e$ and granted its vote to $L$, $(T_L, I_L) \succeq (T_{v^*}, I_{v^*})$, forcing $L$ (by induction over intermediate terms and Log Matching) to contain $e$ at `idx`.
- **AppendEntries Receiver `acked_index` Bound**: Upon accepting `AppendEntries(prev_log_index, entries, leader_commit)`, a follower reconciles entries up to `acked_index = prev_log_index + len(entries)` and updates `commit_index = min(leader_commit, acked_index)`, preventing stale uncommitted tail entries beyond an empty heartbeat from being marked committed or inflating `match_index`.

---

## 2. Chained HotStuff 3-Chain BFT Quorum Intersection (Yin et al., PODC 2019)

Under partial synchrony with $N = 3f + 1$ validators (at most $f$ Byzantine equivocating nodes) and Quorum Certificate threshold $Q = 2f + 1$:

$$|Q_1 \cap Q_2| \ge (2f + 1) + (2f + 1) - (3f + 1) = f + 1$$

Because at most $f$ validators in $Q_1 \cap Q_2$ are Byzantine, **at least $(f + 1) - f = 1$ honest validator** signed both $Q_1$ and $Q_2$. In `helios-consensus` (`N = 7, f = 2, Q = 5`), $|Q_1 \cap Q_2| \ge 3$, guaranteeing at least $3 - 2 = 1$ honest witness (and $3$ total intersecting signers). Direct consecutive 3-chain views $v, v+1, v+2$ (`Prepare -> Pre-Commit -> Commit`) lock honest validators onto `b''`'s ancestor `b` before `b` is finalized upon `QC(b'')`.

---

## 3. Lamping–Veach Jump Consistent Hash Monotonicity & Balance

Given 64-bit key $k$ and shard count $B \ge 1$, Jump Consistent Hash advances bucket index $j$ whenever:

$$j_{\text{next}} = \left\lfloor \frac{(b + 1) \cdot 2^{31}}{\text{LCG}_{64}(k) \bmod 2^{31} + 1} \right\rfloor < B$$

When scaling from $B$ to $B + 1$ shards (`12 -> 13`), a key moves if and only if it jumps specifically to shard $B$, occurring with exact probability $\frac{1}{B + 1} = \frac{1}{13} \approx 0.076923$ (measured on `20,000` keys in C++20: `0.07660`, with `0` cross-language mismatches).
