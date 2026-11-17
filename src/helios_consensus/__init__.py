"""HeliosConsensus: library-style Raft, Jepsen fault-injection simulator, WGL linearizability & C++20 WAL."""

from __future__ import annotations

from helios_consensus.core import (
    ConfChange,
    ConfChangeType,
    Membership,
    PosixDiskFS,
    RaftConfig,
    RaftNode,
    SegmentedWAL,
    SimulatedDiskFS,
    Snapshot,
)
from helios_consensus.multiraft import MultiRaftCluster
from helios_consensus.net import RaftKVClient, RaftTCPServer
from helios_consensus.sim.campaigns import run_jepsen_campaign, run_mutant_killing_suite

__version__ = "1.1.0"

__all__ = [
    "ConfChange",
    "ConfChangeType",
    "Membership",
    "MultiRaftCluster",
    "PosixDiskFS",
    "RaftConfig",
    "RaftKVClient",
    "RaftNode",
    "RaftTCPServer",
    "SegmentedWAL",
    "SimulatedDiskFS",
    "Snapshot",
    "__version__",
    "run_jepsen_campaign",
    "run_mutant_killing_suite",
]
