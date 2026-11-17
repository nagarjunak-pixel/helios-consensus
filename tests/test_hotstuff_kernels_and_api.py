"""Tests for HotStuff 3-Chain BFT, C++20 Jump-Hash/CRC32C kernels, Ollama CEGIS, API, and CLI."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import numpy as np
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from typer.testing import CliRunner

from helios_consensus.api.app import create_app
from helios_consensus.audit.ollama_consensus import (
    build_consensus_incident_suite,
    run_real_consensus_ollama_audit,
)
from helios_consensus.cli import app as cli_app
from helios_consensus.hotstuff.bft_qc import HotStuffValidatorCluster, run_hotstuff_3chain_benchmark
from helios_consensus.kernels.native_ring import (
    HeliosKernelBridge,
    benchmark_jump_hash_and_crc32c,
    get_helios_bridge,
)


def test_hotstuff_3chain_bft_and_cpp20_jump_hash_crc32c_parity() -> None:
    with pytest.raises(ValueError, match="byzantine_f"):
        HotStuffValidatorCluster(byzantine_f=0)

    bft = run_hotstuff_3chain_benchmark(byzantine_f=2, num_blocks=6)
    assert bft.safety_certified is True
    assert bft.honest_quorum_overlap_min >= 1
    assert bft.equivocation_attempts_rejected > 0

    cpp_b = get_helios_bridge()
    py_b = HeliosKernelBridge(csrc_path=Path("/tmp/missing_helios.cpp"))
    assert cpp_b.is_native_cpp is True
    assert py_b.is_native_cpp is False

    with pytest.raises(ValueError, match="num_buckets"):
        cpp_b.jump_hash(123, 0)
    with pytest.raises(ValueError, match="num_buckets"):
        cpp_b.jump_hash_batch(np.array([1, 2], dtype=np.uint64), 0)

    # RFC-3720 / iSCSI standard Castagnoli CRC32C test vector for ASCII b"123456789" == 0xE3069283!
    assert cpp_b.crc32c(b"123456789") == 0xE3069283
    assert py_b.crc32c(b"123456789") == 0xE3069283

    for k in (0, 1, 42, 999999999999):
        assert cpp_b.jump_hash(k, 16) == py_b.jump_hash(k, 16)

    wal_rep = benchmark_jump_hash_and_crc32c()
    assert wal_rep.cpp_vs_python_hash_mismatches == 0
    assert wal_rep.bitflip_corruptions_caught == wal_rep.wal_frames_verified


@settings(max_examples=30, deadline=None)
@given(
    key=st.integers(min_value=0, max_value=(1 << 63) - 1),
    b_small=st.integers(min_value=1, max_value=24),
)
def test_hypothesis_jump_consistent_hash_monotonic_stability_property(
    key: int, b_small: int
) -> None:
    """When expanding from B to B+1 buckets, a key MUST either stay in its old bucket (< B)
    or jump to the newly added bucket B—never moving laterally between old buckets!
    """
    bridge = get_helios_bridge()
    old_b = bridge.jump_hash(key, b_small)
    new_b = bridge.jump_hash(key, b_small + 1)
    assert 0 <= old_b < b_small
    assert new_b in (old_b, b_small)


def test_consensus_cegis_validators_and_mocked_ollama(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    specs = build_consensus_incident_suite()
    assert len(specs) == 6
    # Exercise non-minimal, violation, and exact branches of every formal validator
    for sp in specs:
        assert sp.validator_fn(sp.expected_value) is None
        assert sp.validator_fn(sp.expected_value - 2) is not None
        assert sp.validator_fn(sp.expected_value + 3) is not None

    out_path = tmp_path / "cons_audit.json"
    turn_box = {"calls": 0}

    def fake_get(url: str, timeout: float = 2.0) -> httpx.Response:
        return httpx.Response(200, json={"models": []}, request=httpx.Request("GET", url))

    def fake_post(url: str, json: dict[str, Any], timeout: float = 45.0) -> httpx.Response:
        turn_box["calls"] += 1
        prompt = str(json.get("prompt", ""))
        req = httpx.Request("POST", url)
        # First attempt fails if no COUNTEREXAMPLE present; repair turn returns exact safe value!
        if "COUNTEREXAMPLE" not in prompt:
            return httpx.Response(
                200, json={"response": '{"steps": ["guess"], "value": 2}'}, request=req
            )
        for sp in specs:
            if sp.prompt in prompt:
                return httpx.Response(
                    200,
                    json={"response": f'{{"steps": ["repaired"], "value": {sp.expected_value}}}'},
                    request=req,
                )
        return httpx.Response(200, json={"response": "{}"}, request=req)

    monkeypatch.setattr(httpx, "get", fake_get)
    monkeypatch.setattr(httpx, "post", fake_post)

    live_rep = run_real_consensus_ollama_audit(save_path=out_path, allow_offline_replay=False)
    assert live_rep.raw_safe_pct == 0.0
    assert live_rep.verified_safe_pct == 100.0
    assert live_rep.zero_split_brain_leakage is True

    def offline_get(url: str, timeout: float = 2.0) -> httpx.Response:
        raise httpx.ConnectError("offline")

    monkeypatch.setattr(httpx, "get", offline_get)
    replay = run_real_consensus_ollama_audit(save_path=out_path, allow_offline_replay=True)
    assert replay.verified_safe_count == 6

    with pytest.raises(RuntimeError, match="unreachable"):
        run_real_consensus_ollama_audit(
            save_path=tmp_path / "missing.json", allow_offline_replay=False
        )


async def test_fastapi_endpoints_and_cli_commands(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    app = create_app()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        assert (await client.get("/healthz")).status_code == 200
        assert (await client.get("/api/v1/overview")).status_code == 200
        assert (
            await client.post("/api/v1/raft/simulate", json={"cluster_size": 4})
        ).status_code == 200
        assert (
            await client.post("/api/v1/raft/simulate", json={"cluster_size": 5})
        ).status_code == 200
        assert (await client.get("/api/v1/modelcheck/run")).status_code == 200
        assert (
            await client.post("/api/v1/hotstuff/simulate", json={"byzantine_f": 1, "num_blocks": 3})
        ).status_code == 200
        assert (await client.get("/api/v1/kernels/benchmark")).status_code == 200
        assert (
            await client.post(
                "/api/v1/jepsen/run",
                json={"num_seeds": 2, "ticks": 300, "use_segmented_wal": True},
            )
        ).status_code == 200
        assert (await client.get("/api/v1/mutants/run")).status_code == 200
        assert (await client.get("/")).status_code == 200

    headless = create_app(ui_dist_dir=tmp_path / "empty_ui")
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=headless), base_url="http://test"
    ) as hc:
        assert (await hc.get("/")).json()["mode"] == "headless-api"

    runner = CliRunner()
    for args in (
        ["jepsen", "--seeds", "2", "--ticks", "300"],
        ["jepsen", "--seeds", "2", "--ticks", "300", "--json"],
        ["kill-mutants"],
        ["kill-mutants", "--json"],
        ["multiraft"],
        ["multiraft", "--json"],
        ["simulate-raft"],
        ["simulate-raft", "--json"],
        ["model-check"],
        ["model-check", "--json"],
        ["simulate-hotstuff"],
        ["simulate-hotstuff", "--json"],
        ["benchmark-kernels"],
        ["benchmark-kernels", "--json"],
        ["demo"],
        ["demo", "--json"],
    ):
        assert runner.invoke(cli_app, args).exit_code == 0

    saved = tmp_path / "cons_saved.json"
    saved.write_text(
        json.dumps(
            {
                "model_name": "qwen3.5:4b",
                "num_incidents": 1,
                "raw_safe_count": 0,
                "raw_safe_pct": 0.0,
                "verified_safe_count": 1,
                "verified_safe_pct": 100.0,
                "safety_lift_pct": 100.0,
                "zero_split_brain_leakage": True,
                "outcomes": [
                    {
                        "incident_id": "HC-01",
                        "title": "Test",
                        "expected_safe_quorum_or_fencing": 17,
                        "raw_llm_value": 15,
                        "raw_llm_safe": False,
                        "model_checked_value": 17,
                        "model_checked_safe": True,
                        "turns_used": 2,
                        "latency_ms": 200.0,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    def offline_get(url: str, timeout: float = 2.0) -> httpx.Response:
        raise httpx.ConnectError("offline")

    monkeypatch.setattr(httpx, "get", offline_get)
    assert runner.invoke(cli_app, ["eval-ollama", "--save-path", str(saved)]).exit_code == 0
    assert (
        runner.invoke(cli_app, ["eval-ollama", "--save-path", str(saved), "--json"]).exit_code == 0
    )
