"""Resilient ``asyncio`` KV client with automatic leader redirect and session retries."""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

from helios_consensus.core.messages import ConfChange
from helios_consensus.net.codec import pack_envelope, read_envelope


class ClusterUnavailableError(RuntimeError):
    """Raised when a request exhausts its retry budget across all cluster members."""


class RaftKVClient:
    """Linearizable KV client tracking leader hints and monotonic ``(client_id, seq)`` IDs."""

    def __init__(
        self,
        endpoints: dict[int, tuple[str, int]],
        *,
        client_id: int = 1,
        rpc_timeout: float = 0.8,
        max_attempts: int = 25,
    ) -> None:
        self.endpoints: dict[int, tuple[str, int]] = dict(endpoints)
        self.client_id = client_id
        self.seq = 0
        self.rpc_timeout = rpc_timeout
        self.max_attempts = max_attempts
        self.leader_hint: int | None = min(endpoints) if endpoints else None

    def update_endpoints(self, endpoints: dict[int, tuple[str, int]]) -> None:
        self.endpoints.update(endpoints)

    async def _rpc_once(self, node_id: int, payload: dict[str, Any]) -> dict[str, Any] | None:
        addr = self.endpoints.get(node_id)
        if addr is None:
            return None
        writer: asyncio.StreamWriter | None = None
        try:
            reader, w = await asyncio.wait_for(
                asyncio.open_connection(addr[0], addr[1]), timeout=self.rpc_timeout
            )
            writer = w
            w.write(pack_envelope(payload))
            await w.drain()
            return await asyncio.wait_for(read_envelope(reader), timeout=self.rpc_timeout)
        except Exception:
            return None
        finally:
            if writer is not None:
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()

    async def _call_with_retry(self, payload: dict[str, Any]) -> dict[str, Any]:
        node_ids = sorted(self.endpoints)
        if not node_ids:
            raise ClusterUnavailableError("no cluster endpoints configured")
        cursor = 0
        for attempt in range(self.max_attempts):
            target = (
                self.leader_hint
                if self.leader_hint in self.endpoints
                else node_ids[cursor % len(node_ids)]
            )
            resp = await self._rpc_once(target, payload)
            if resp is not None and resp.get("ok") is True:
                self.leader_hint = target
                return resp

            hint = resp.get("leader_hint") if isinstance(resp, dict) else None
            if isinstance(hint, int) and hint in self.endpoints and hint != target:
                self.leader_hint = hint
            else:
                cursor += 1
                self.leader_hint = node_ids[cursor % len(node_ids)]
            await asyncio.sleep(min(0.08, 0.015 * (attempt + 1)))
        raise ClusterUnavailableError(
            f"exhausted {self.max_attempts} attempts for {payload.get('op')}"
        )

    async def put(self, key: str, value: int) -> bool:
        self.seq += 1
        resp = await self._call_with_retry(
            {
                "kind": "client",
                "op": "put",
                "key": key,
                "value": value,
                "client": self.client_id,
                "seq": self.seq,
                "timeout": self.rpc_timeout,
            }
        )
        return bool(resp["result"])

    async def cas(self, key: str, expected: int | None, value: int) -> bool:
        self.seq += 1
        resp = await self._call_with_retry(
            {
                "kind": "client",
                "op": "cas",
                "key": key,
                "expected": expected,
                "value": value,
                "client": self.client_id,
                "seq": self.seq,
                "timeout": self.rpc_timeout,
            }
        )
        return bool(resp["result"])

    async def get(self, key: str) -> int | None:
        resp = await self._call_with_retry(
            {
                "kind": "client",
                "op": "get",
                "key": key,
                "timeout": self.rpc_timeout,
            }
        )
        val = resp["value"]
        return None if val is None else int(val)

    async def propose_conf_change(self, cc: ConfChange) -> int:
        resp = await self._call_with_retry(
            {
                "kind": "client",
                "op": "conf_change",
                "conf_change": cc.encode(),
            }
        )
        return int(resp["index"])

    async def status(self, node_id: int) -> dict[str, Any] | None:
        resp = await self._rpc_once(node_id, {"kind": "client", "op": "status"})
        if resp and resp.get("ok"):
            out: dict[str, Any] = resp["status"]
            return out
        return None
