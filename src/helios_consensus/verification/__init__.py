"""TLA+ / TLC-style explicit-state breadth-first model checker for consensus safety invariants."""

from helios_consensus.verification.model_checker import explore_consensus_state_space

__all__ = ["explore_consensus_state_space"]
