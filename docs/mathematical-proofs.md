# Mathematical Proofs & Consensus Invariants in `helios-consensus`

## 1. Raft Election Restriction & Current-Term Commit Rule (Ongaro & Ousterhout §5.4.1–§5.4.2)

Let a cluster of odd size $N = 2k + 1$ require strict majorities $Q = k + 1$.
For any two majorities $Q_1, Q_2 \subseteq \{1, \dots, N\}$:

$$|Q_1 \cap Q_2| = |Q_1| + |Q_2| - |Q_1 \cup Q_2| \ge (k + 1) + (k + 1) - (2k + 1) = 1$$

- **Election Safety**: Because each node persists `voted_for` at most once per term $t$ before replying, two distinct candidates cannot both assemble $k + 1$ votes in term $t$.
- **Up-to-Date Log Partial Order (§5.4.1)**: Define $(T_a, I_a) \succeq (T_b, I_b) \iff (T_a > T_b) \lor (T_a = T_b \land I_a \ge I_b)$. A voter $v$ grants `RequestVote` to candidate $c$ only if $(T_c, I_c) \succeq (T_v, I_v)$.
- **Leader Completeness & Figure 8 (§5.4.2)**: An entry from a prior term $t_0 < t$ replicated onto a majority in term $t$ is **not** safe to commit by counting replicas alone (`Figure 8`), because a candidate holding a conflicting entry with term $t_1 \in (t_0, t)$ at the same index could still out-rank those followers under $\succeq$. Committing at least one entry from the leader's current term $t$ seals all preceding entries under Log Matching.

---

## 2. Joint Consensus Dual-Quorum Transition Safety (Ongaro §6)

To transition safely from arbitrary voter set $C_{\text{old}}$ to $C_{\text{new}}$ (including completely disjoint sets such as $\{1,2,3\} \to \{4,5,6\}$), single-step switching creates a window where two disjoint majorities ($Q_{\text{old}} \subseteq C_{\text{old}}$ and $Q_{\text{new}} \subseteq C_{\text{new}}$) can elect concurrent leaders.

`helios-consensus` implements two-phase Joint Consensus $C_{\text{old,new}}$:

$$\text{Quorum}_{C_{\text{old,new}}}(A) \iff |A \cap C_{\text{old}}| \ge \left\lfloor \frac{|C_{\text{old}}|}{2} \right\rfloor + 1 \;\land\; |A \cap C_{\text{new}}| \ge \left\lfloor \frac{|C_{\text{new}}|}{2} \right\rfloor + 1$$

1. Before $C_{\text{old,new}}$ commits, any leader elected under $C_{\text{old}}$ must win a $C_{\text{old}}$ majority, which intersects any $C_{\text{old,new}}$ dual quorum in at least one voter.
2. Once $C_{\text{old,new}}$ is committed, only a candidate holding $C_{\text{old,new}}$ can win both majorities; the leader then appends `LEAVE_JOINT` ($C_{\text{new}}$), and after $C_{\text{new}}$ commits, $C_{\text{old}} \setminus C_{\text{new}}$ replicas are safely decommissioned.
3. Commit watermarks during $C_{\text{old,new}}$ satisfy $I_{\text{commit}} = \min(\text{median}(C_{\text{old}}), \text{median}(C_{\text{new}}))$.

---

## 3. Wing–Gong–Lowe Register Linearizability (`P`-Compositionality)

By Herlihy & Wing's locality theorem, a multi-key history $H$ is linearizable if and only if every single-key projection $H|_k$ is linearizable. For a key history $H|_k$ of $n$ operations $o_i = (\text{call}_i, \text{ret}_i, \text{op}_i)$ (with $\text{ret}_i = +\infty$ for timed-out indeterminate writes/CAS):

- An operation $o_i$ is **minimal unlinearized** when $\text{call}_i < \min_{o_j \notin L} \text{ret}_j$.
- Search state is uniquely determined by the pair $(L, s) \in \{0,1\}^n \times (\mathbb{Z} \cup \{\bot\})$, where $L$ is the bitmask of linearized operations and $s$ is the current register value. Lowe's memoization cache prunes revisited $(L, s)$ pairs, turning exponential permutation blowups under concurrent writes into a compact state-graph traversal (`<= 62` operations encoded in a single `uint64_t` in C++20).

---

## 4. Chained HotStuff 3-Chain BFT Quorum Intersection & Jump Consistent Hash

- **HotStuff (`N = 3f + 1, Q = 2f + 1`)**: Any two Quorum Certificates intersect in $|Q_1 \cap Q_2| \ge (2f+1) + (2f+1) - (3f+1) = f + 1$ validators, guaranteeing at least $(f+1) - f = 1$ honest validator across every quorum pair.
- **Jump Consistent Hash**: Scaling from $B$ to $B+1$ buckets moves a key solely when its pseudo-random jump lands in bucket $B$, with exact migration probability $\frac{1}{B+1}$.
- **RFC-6962 Domain-Separated Merkle Tree**: Snapshot chunks $c_0, \dots, c_{m-1}$ use leaf hashes $h_i = \text{SHA256}(\texttt{0x00} \parallel c_i)$ and internal hashes $\text{SHA256}(\texttt{0x01} \parallel L \parallel R)$, preventing second-preimage leaf/interior confusion during chunked `InstallSnapshot` streaming.
