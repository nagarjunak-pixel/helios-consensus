"""Zero-Mock Live Ollama Consensus Quorum & Split-Brain Fencing Formal Synthesis Audit."""

from helios_consensus.audit.ollama_consensus import (
    default_consensus_artifact_path,
    run_real_consensus_ollama_audit,
)

__all__ = [
    "default_consensus_artifact_path",
    "run_real_consensus_ollama_audit",
]
