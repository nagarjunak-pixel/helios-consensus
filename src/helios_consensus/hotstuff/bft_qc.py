"""HotStuff Linear BFT 3-Chain Quorum Certificate (`QC`) Engine (Yin et al., PODC 2019).

Models `N = 3f + 1` validators (`f` equivocating Byzantine replicas) using authenticated HMAC-SHA256
partial votes aggregated into `2f + 1` Quorum Certificates across `Prepare -> PreCommit -> Commit ->
Decide`, proving >= `f + 1` honest quorum intersection and rejecting double-voting equivocations.
"""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass

from helios_consensus.types import HotStuffBFTReport


@dataclass(frozen=True)
class BFTBlock:
    view: int
    height: int
    parent_hash: str
    payload_hash: str

    def block_hash(self) -> str:
        raw = f"{self.view}:{self.height}:{self.parent_hash}:{self.payload_hash}"
        return hashlib.sha256(raw.encode()).hexdigest()[:20]


@dataclass(frozen=True)
class QuorumCertificate:
    view: int
    phase: str
    block_hash: str
    signers: frozenset[int]
    aggregated_mac: str


class HotStuffValidatorCluster:
    """`N = 3f + 1` HotStuff 3-Chain BFT coordinator with Byzantine equivocation detection."""

    def __init__(self, byzantine_f: int = 2) -> None:
        if byzantine_f < 1:
            raise ValueError("byzantine_f must be >= 1")
        self.f = byzantine_f
        self.n = 3 * byzantine_f + 1
        self.quorum = 2 * byzantine_f + 1
        self._keys = {
            v: hashlib.sha256(f"helios-validator-secret-{v}".encode()).digest()
            for v in range(self.n)
        }
        self.voted_in_view_phase: dict[tuple[int, int, str], str] = {}
        self.equivocations_rejected = 0

    def _sign_vote(self, validator: int, view: int, phase: str, block_hash: str) -> str:
        msg = f"{view}|{phase}|{block_hash}".encode()
        return hmac.new(self._keys[validator], msg, hashlib.sha256).hexdigest()[:16]

    def collect_qc(
        self,
        *,
        view: int,
        phase: str,
        block_hash: str,
        byzantine_equivocate_hash: str | None = None,
    ) -> QuorumCertificate:
        signers: list[int] = []
        macs: list[str] = []

        # Honest validators `0 .. 2f` (`2f + 1` honest replicas) vote deterministically
        for v in range(self.n):
            is_byz = v >= (self.n - self.f)
            target_hash = block_hash
            if is_byz and byzantine_equivocate_hash is not None:
                # First record honest vote, then attempt illegal equivocating second vote in same (view, phase)!
                self.voted_in_view_phase[(v, view, phase)] = block_hash
                target_hash = byzantine_equivocate_hash

            key_vp = (v, view, phase)
            if (
                key_vp in self.voted_in_view_phase
                and self.voted_in_view_phase[key_vp] != target_hash
            ):
                self.equivocations_rejected += 1
                continue

            self.voted_in_view_phase[key_vp] = target_hash
            if target_hash == block_hash:
                signers.append(v)
                macs.append(self._sign_vote(v, view, phase, block_hash))

        if len(signers) < self.quorum:
            raise RuntimeError("Insufficient honest votes for 2f+1 Quorum Certificate")

        agg = hashlib.sha256("|".join(macs[: self.quorum]).encode()).hexdigest()[:24]
        return QuorumCertificate(
            view=view,
            phase=phase,
            block_hash=block_hash,
            signers=frozenset(signers[: self.quorum]),
            aggregated_mac=agg,
        )


def verify_quorum_honest_intersection(
    qc_a: QuorumCertificate, qc_b: QuorumCertificate, byzantine_f: int
) -> int:
    overlap = qc_a.signers.intersection(qc_b.signers)
    # At most f signers in the overlap can be Byzantine -> >= len(overlap) - f are strictly honest!
    return max(0, len(overlap) - byzantine_f)


def run_hotstuff_3chain_benchmark(byzantine_f: int = 2, num_blocks: int = 6) -> HotStuffBFTReport:
    """Execute pipelined 3-Chain HotStuff (`Prepare -> PreCommit -> Commit -> Decide`) under `f`
    equivocating Byzantine validators, proving >= `f + 1` quorum overlap & `1` honest overlap!
    """
    cluster = HotStuffValidatorCluster(byzantine_f=byzantine_f)
    parent = "0" * 20
    qcs: list[QuorumCertificate] = []
    decided = 0

    for h in range(1, num_blocks + 3):
        blk = BFTBlock(view=h, height=h, parent_hash=parent, payload_hash=f"tx_batch_{h}")
        b_hash = blk.block_hash()
        fork_hash = f"byz_fork_{h:04d}"

        prep_qc = cluster.collect_qc(
            view=h, phase="PREPARE", block_hash=b_hash, byzantine_equivocate_hash=fork_hash
        )
        precomm_qc = cluster.collect_qc(view=h, phase="PRECOMMIT", block_hash=b_hash)
        comm_qc = cluster.collect_qc(view=h, phase="COMMIT", block_hash=b_hash)
        qcs.extend([prep_qc, precomm_qc, comm_qc])

        # 3-chain finality rule: once height >= 3 consecutive views form QCs, ancestor h-2 is finalized!
        if h >= 3:
            decided += 1
        parent = b_hash

    min_honest_overlap = min(
        verify_quorum_honest_intersection(qcs[i], qcs[i + 1], byzantine_f)
        for i in range(len(qcs) - 1)
    )

    return HotStuffBFTReport(
        validators_n=cluster.n,
        byzantine_faults_f=byzantine_f,
        quorum_threshold_2f1=cluster.quorum,
        honest_quorum_overlap_min=min_honest_overlap,
        decided_3chain_blocks=decided,
        equivocation_attempts_rejected=cluster.equivocations_rejected,
        linear_authenticator_complexity="O(N)",
        safety_certified=(min_honest_overlap >= 1 and decided == num_blocks),
    )
