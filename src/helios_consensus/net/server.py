"""Real ``asyncio`` TCP server hosting a ``RaftNode``, ``Storage``, and ``KVStateMachine``."""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

from helios_consensus.core.messages import ConfChange, Message, MsgType, Snapshot
from helios_consensus.core.node import (
    NotLeaderError,
    ProposalDroppedError,
    RaftConfig,
    RaftNode,
)
from helios_consensus.core.snapshot_stream import SnapshotAssembler, split_snapshot
from helios_consensus.core.storage import Storage
from helios_consensus.kv.state_machine import KVCommand, KVOp, KVStateMachine
from helios_consensus.net.codec import (
    WireFrameError,
    chunk_from_dict,
    chunk_to_dict,
    message_from_dict,
    message_to_dict,
    pack_envelope,
    read_envelope,
)


class RaftTCPServer:
    """Single Raft replica serving peer consensus RPCs and client KV requests over TCP."""

    def __init__(
        self,
        node_id: int,
        peers: dict[int, tuple[str, int]],
        storage: Storage,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        config: RaftConfig | None = None,
        tick_interval: float = 0.012,
        snapshot_threshold: int = 40,
        snapshot_chunk_bytes: int = 2048,
        seed: int = 1,
    ) -> None:
        self.node_id = node_id
        self.peers: dict[int, tuple[str, int]] = dict(peers)
        self.storage = storage
        self.host = host
        self.port = port
        self.cfg = config or RaftConfig(election_tick=12, heartbeat_tick=2)
        self.tick_interval = tick_interval
        self.snapshot_threshold = snapshot_threshold
        self.snapshot_chunk_bytes = snapshot_chunk_bytes

        hs, snap, entries = storage.load()
        self.raft = RaftNode(
            node_id,
            self.cfg,
            seed=seed,
            snapshot=snap,
            hard_state=hs,
            entries=entries,
        )
        self.sm = KVStateMachine.restore(snap.data)
        self._assemblers: dict[int, SnapshotAssembler] = {}

        self._server: asyncio.AbstractServer | None = None
        self._tick_task: asyncio.Task[None] | None = None
        self._peer_writers: dict[int, asyncio.StreamWriter] = {}
        self._inbound_writers: set[asyncio.StreamWriter] = set()
        self._write_waiters: dict[tuple[int, int], asyncio.Future[bool]] = {}
        self._read_waiters: dict[int, tuple[str, asyncio.Future[int | None]]] = {}
        self._ready_reads: list[tuple[int, int]] = []
        self._req_id_seq = 0
        self._closed = False

    @property
    def address(self) -> tuple[str, int]:
        return (self.host, self.port)

    def update_peers(self, peers: dict[int, tuple[str, int]]) -> None:
        self.peers.update(peers)

    async def start(self) -> tuple[str, int]:
        self._closed = False
        self._server = await asyncio.start_server(self._handle_conn, self.host, self.port)
        sock = self._server.sockets[0]
        bound = sock.getsockname()
        self.port = int(bound[1])
        self.peers[self.node_id] = (self.host, self.port)
        self._tick_task = asyncio.create_task(self._tick_loop())
        return (self.host, self.port)

    async def stop(self) -> None:
        self._closed = True
        if self._tick_task is not None:
            self._tick_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._tick_task
            self._tick_task = None
        for writer in list(self._peer_writers.values()):
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
        self._peer_writers.clear()
        for in_writer in list(self._inbound_writers):
            in_writer.close()
        if self._server is not None:
            self._server.close()
            if hasattr(self._server, "close_clients"):
                self._server.close_clients()
            await self._server.wait_closed()
            self._server = None
        for fut in self._write_waiters.values():
            if not fut.done():
                fut.cancel()
        self._write_waiters.clear()
        for _, rfut in self._read_waiters.values():
            if not rfut.done():
                rfut.cancel()
        self._read_waiters.clear()

    # --------------------------------------------------------------------- tick & ready
    async def _tick_loop(self) -> None:
        while not self._closed:
            await asyncio.sleep(self.tick_interval)
            self.raft.tick()
            await self._drain_ready()

    async def _drain_ready(self) -> None:
        while self.raft.has_ready():
            rd = self.raft.ready()
            self.storage.save(rd)
            out_msgs = list(rd.messages)
            if rd.snapshot is not None:
                self.sm = KVStateMachine.restore(rd.snapshot.data)
            for entry in rd.committed_entries:
                res = self.sm.apply(entry)
                if res is not None:
                    fut = self._write_waiters.pop((res.client, res.seq), None)
                    if fut is not None and not fut.done():
                        fut.set_result(res.result)
            for rs in rd.read_states:
                self._ready_reads.append((rs.request_id, rs.index))
            self.raft.advance(rd)
            self._complete_ready_reads()
            self._maybe_snapshot()
            for msg in out_msgs:
                asyncio.create_task(self._send_peer(msg))

    def _complete_ready_reads(self) -> None:
        still_waiting: list[tuple[int, int]] = []
        for req_id, req_index in self._ready_reads:
            if self.sm.applied_index >= req_index:
                waiter = self._read_waiters.pop(req_id, None)
                if waiter is not None:
                    key, fut = waiter
                    if not fut.done():
                        fut.set_result(self.sm.read(key))
            else:
                still_waiting.append((req_id, req_index))
        self._ready_reads = still_waiting

    def _maybe_snapshot(self) -> None:
        applied = self.raft.log.applied
        if applied - self.raft.log.snapshot.index >= self.snapshot_threshold:
            snap = self.raft.compact(applied, self.sm.snapshot())
            self.storage.save_snapshot(snap)

    # --------------------------------------------------------------------- peer IO
    async def _get_writer(self, dst: int) -> asyncio.StreamWriter | None:
        existing = self._peer_writers.get(dst)
        if existing is not None and not existing.is_closing():
            return existing
        addr = self.peers.get(dst)
        if addr is None:
            return None
        try:
            _, writer = await asyncio.wait_for(
                asyncio.open_connection(addr[0], addr[1]), timeout=0.4
            )
            self._peer_writers[dst] = writer
            return writer
        except Exception:
            return None

    async def _send_peer(self, msg: Message) -> None:
        if self._closed:
            return
        writer = await self._get_writer(msg.dst)
        if writer is None:
            return
        try:
            if msg.type is MsgType.SNAPSHOT and msg.snapshot is not None:
                chunks = split_snapshot(
                    msg.snapshot, term=msg.term, chunk_bytes=self.snapshot_chunk_bytes
                )
                for ch in chunks:
                    writer.write(
                        pack_envelope(
                            {
                                "kind": "snap_chunk",
                                "src": msg.src,
                                "dst": msg.dst,
                                "chunk": chunk_to_dict(ch),
                            }
                        )
                    )
                await writer.drain()
            else:
                writer.write(pack_envelope({"kind": "raft", "msg": message_to_dict(msg)}))
                await writer.drain()
        except Exception:
            self._peer_writers.pop(msg.dst, None)
            with contextlib.suppress(Exception):
                writer.close()

    # --------------------------------------------------------------------- inbound RPCs
    async def _handle_conn(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._inbound_writers.add(writer)
        try:
            while not self._closed:
                env = await read_envelope(reader)
                if env is None:
                    break
                kind = env.get("kind")
                if kind == "raft":
                    msg = message_from_dict(env["msg"])
                    if msg.dst == self.node_id:
                        self.raft.step(msg)
                        await self._drain_ready()
                elif kind == "snap_chunk":
                    src = int(env["src"])
                    ch = chunk_from_dict(env["chunk"])
                    asm = self._assemblers.setdefault(src, SnapshotAssembler())
                    _, completed_snap = asm.ingest(ch)
                    if completed_snap is not None:
                        asm.reset()
                        self._deliver_reassembled_snapshot(src, ch.term, completed_snap)
                        await self._drain_ready()
                elif kind == "client":
                    reply = await self._handle_client_rpc(env)
                    writer.write(pack_envelope(reply))
                    await writer.drain()
        except (WireFrameError, ConnectionError, asyncio.CancelledError):
            pass
        finally:
            self._inbound_writers.discard(writer)
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    def _deliver_reassembled_snapshot(self, src: int, term: int, snap: Snapshot) -> None:
        self.raft.step(
            Message(
                type=MsgType.SNAPSHOT,
                src=src,
                dst=self.node_id,
                term=term,
                snapshot=snap,
            )
        )

    async def _handle_client_rpc(self, env: dict[str, Any]) -> dict[str, Any]:
        op = str(env.get("op", ""))
        if op == "status":
            return {"ok": True, "status": self.raft.status()}

        try:
            if op in ("put", "cas"):
                client_id = int(env["client"])
                seq = int(env["seq"])
                cmd = KVCommand(
                    op=KVOp.PUT if op == "put" else KVOp.CAS,
                    key=str(env["key"]),
                    value=int(env["value"]),
                    expected=env.get("expected"),
                    client=client_id,
                    seq=seq,
                )
                # Fast path if this client/seq was already applied before retry.
                prior = self.sm.sessions.get(client_id)
                if prior is not None and prior[0] >= seq:
                    return {"ok": True, "result": prior[1]}
                loop = asyncio.get_running_loop()
                fut: asyncio.Future[bool] = loop.create_future()
                self._write_waiters[(client_id, seq)] = fut
                self.raft.propose(cmd.encode())
                await self._drain_ready()
                res = await asyncio.wait_for(fut, timeout=float(env.get("timeout", 1.5)))
                return {"ok": True, "result": res}

            if op == "get":
                self._req_id_seq += 1
                req_id = self._req_id_seq
                loop = asyncio.get_running_loop()
                rfut: asyncio.Future[int | None] = loop.create_future()
                self._read_waiters[req_id] = (str(env["key"]), rfut)
                self.raft.read_index(req_id)
                await self._drain_ready()
                val = await asyncio.wait_for(rfut, timeout=float(env.get("timeout", 1.5)))
                return {"ok": True, "value": val}

            if op == "conf_change":
                cc = ConfChange.decode(str(env["conf_change"]))
                idx = self.raft.propose_conf_change(cc)
                await self._drain_ready()
                return {"ok": True, "index": idx}

            return {"ok": False, "error": f"unknown_op:{op}"}
        except NotLeaderError as exc:
            return {"ok": False, "error": "not_leader", "leader_hint": exc.leader_hint}
        except ProposalDroppedError as exc:
            return {"ok": False, "error": "proposal_dropped", "detail": str(exc)}
        except TimeoutError:
            return {"ok": False, "error": "timeout", "leader_hint": self.raft.lead}
