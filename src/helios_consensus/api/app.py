"""FastAPI control plane and Raft DST, TLA+ Model Checker & HotStuff BFT Workbench."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from helios_consensus.audit.ollama_consensus import default_consensus_artifact_path
from helios_consensus.hotstuff.bft_qc import run_hotstuff_3chain_benchmark
from helios_consensus.kernels.native_ring import benchmark_jump_hash_and_crc32c, get_helios_bridge
from helios_consensus.raft.engine import run_raft_dst_partition_benchmark
from helios_consensus.types import (
    HotStuffBFTReport,
    JumpHashWALReport,
    ModelCheckerReport,
    RaftDSTReport,
    RealConsensusAuditReport,
)
from helios_consensus.verification.model_checker import explore_consensus_state_space


class RaftSimRequest(BaseModel):
    cluster_size: int = Field(default=5, ge=3, le=9)


class HotStuffRequest(BaseModel):
    byzantine_f: int = Field(default=2, ge=1, le=6)
    num_blocks: int = Field(default=6, ge=2, le=20)


_WORKBENCH_CACHE: dict[str, Any] | None = None


def _load_consensus_audit() -> dict[str, Any] | None:
    artifact = default_consensus_artifact_path()
    if not artifact.exists():
        return None
    rep = RealConsensusAuditReport.model_validate(json.loads(artifact.read_text(encoding="utf-8")))
    return rep.model_dump()


def _get_workbench_cache() -> dict[str, Any]:
    global _WORKBENCH_CACHE
    if _WORKBENCH_CACHE is None:
        bridge = get_helios_bridge()
        raft_rep = run_raft_dst_partition_benchmark(cluster_size=5)
        mc_rep = explore_consensus_state_space(max_term=2, max_log_len=2)
        bft_rep = run_hotstuff_3chain_benchmark(byzantine_f=2, num_blocks=6)
        wal_rep = benchmark_jump_hash_and_crc32c()
        _WORKBENCH_CACHE = {
            "service": "helios-consensus",
            "version": "1.0.0",
            "kernel_backend": bridge.backend,
            "is_native_cpp": bridge.is_native_cpp,
            "raft_dst": raft_rep.model_dump(),
            "model_checker": mc_rep.model_dump(),
            "hotstuff_bft": bft_rep.model_dump(),
            "jump_hash_wal": wal_rep.model_dump(),
            "real_qwen_consensus": _load_consensus_audit(),
        }
    else:
        _WORKBENCH_CACHE["real_qwen_consensus"] = _load_consensus_audit()
    return _WORKBENCH_CACHE


def create_app(ui_dist_dir: Path | None = None) -> FastAPI:
    app = FastAPI(
        title="HeliosConsensus Raft DST, TLA+ Model Checker, HotStuff BFT & Native C++20 Engine",
        version="1.0.0",
    )

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        bridge = get_helios_bridge()
        return {
            "status": "ok",
            "service": "helios-consensus",
            "version": "1.0.0",
            "kernel_backend": bridge.backend,
            "is_native_cpp": bridge.is_native_cpp,
        }

    @app.get("/api/v1/overview")
    async def overview() -> dict[str, Any]:
        return _get_workbench_cache()

    @app.post("/api/v1/raft/simulate", response_model=RaftDSTReport)
    async def raft_sim(req: RaftSimRequest) -> RaftDSTReport:
        if req.cluster_size % 2 == 0:
            return run_raft_dst_partition_benchmark(cluster_size=req.cluster_size + 1)
        return run_raft_dst_partition_benchmark(cluster_size=req.cluster_size)

    @app.get("/api/v1/modelcheck/run", response_model=ModelCheckerReport)
    async def mc_run() -> ModelCheckerReport:
        return explore_consensus_state_space(max_term=2, max_log_len=2)

    @app.post("/api/v1/hotstuff/simulate", response_model=HotStuffBFTReport)
    async def bft_sim(req: HotStuffRequest) -> HotStuffBFTReport:
        return run_hotstuff_3chain_benchmark(byzantine_f=req.byzantine_f, num_blocks=req.num_blocks)

    @app.get("/api/v1/kernels/benchmark", response_model=JumpHashWALReport)
    async def wal_bench() -> JumpHashWALReport:
        return benchmark_jump_hash_and_crc32c()

    resolved_dist = ui_dist_dir or (Path(__file__).resolve().parents[3] / "ui" / "dist")
    assets_dir = resolved_dist / "assets"
    if assets_dir.exists():
        app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")

    @app.get("/", response_model=None)
    async def root_page() -> FileResponse | JSONResponse:
        index_html = resolved_dist / "index.html"
        if index_html.exists():
            return FileResponse(index_html)
        return JSONResponse(
            {
                "service": "helios-consensus",
                "mode": "headless-api",
                "docs": "/docs",
                "overview": "/api/v1/overview",
            }
        )

    return app


app = create_app()
