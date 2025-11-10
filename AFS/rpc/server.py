import asyncio
import uuid
import time
from typing import Dict, Callable, Awaitable
from AFS.rpc.framing import read_frame, write_frame
import json
import aiosqlite

OK = 0
E_INTERNAL = 1999
E_TIMEOUT = 1001

Handler = Callable[..., Awaitable[dict]]


class RPCServer:

    def __init__(self) -> None:

        self._handlers: Dict[str, Handler] = {}
        self._db_path = "./srv_data/oplog.db"
        self._db = None

        self._oplog: Dict[str, dict] = {}

    def register(self, op: str, fn: Handler) -> None:
        self._handlers[op] = fn

    async def _init_storage(self) -> None:
        self._db = await aiosqlite.connect(self._db_path)
        await self._db.execute("PRAGMA journal_mode=WAL;")
        await self._db.execute(
            "CREATE TABLE IF NOT EXISTS op_log ("
            "  op_id TEXT PRIMARY KEY,"
            "  result_json TEXT NOT NULL,"
            "  created_at INTEGER NOT NULL"
            ")"
        )
        await self._db.commit()

    def _log(self, peer, req_id: str, op: str, stage: str, **kwargs) -> None:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        peer_str = f"{peer[0]}:{peer[1]}" if peer else "unknown"
        parts = [f"[{timestamp}]", f"[{peer_str}]", f"[{req_id[:8]}]", f"[{op}]", f"[{stage}]"]
        for key, value in kwargs.items():
            parts.append(f"{key}={value}")
        print(" ".join(parts))

    async def _handle_conn(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        print(f"[server] client connected: {peer}")

        try:
            while True:
                req = await read_frame(reader)
                op = req.get("op")
                args = req.get("args", {}) or {}
                rid = req.get("req_id", str(uuid.uuid4()))
                self._log(peer, rid, op, "START", args_count=len(args))
                op_id = args.pop("op_id", None)
                if op_id is not None:
                    cur = await self._db.execute("SELECT result_json FROM op_log WHERE op_id = ?", (op_id,))
                    row = await cur.fetchone()
                    await cur.close()
                    if row:
                        prev = json.loads(row[0])
                        resp = {"req_id": rid, "code": OK, "err": "", "data": prev, "ms": 0}

                        self._log(peer, rid, op, "CACHED", op_id=op_id)
                        await write_frame(writer, resp)
                        continue

                t_req = time.perf_counter()
                deadline_ms = req.get("deadline_ms", None)

                fn = self._handlers.get(op)
                if fn is None:
                    resp = {"req_id": rid, "code": E_INTERNAL, "err": f"unknown op '{op}'"}

                    self._log(peer, rid, op, "ERROR", reason="unknown_op")
                    await write_frame(writer, resp)
                    continue

                try:

                    self._log(peer, rid, op, "EXECUTING", deadline_ms=deadline_ms)

                    if isinstance(deadline_ms, (int, float)):
                        remain = (deadline_ms / 1000.0) - (time.perf_counter() - t_req)
                        if remain <= 0:
                            resp = {"req_id": rid, "code": E_TIMEOUT, "err": "DEADLINE_EXCEEDED"}

                            self._log(peer, rid, op, "TIMEOUT", reason="deadline_exceeded_before_exec")
                        else:
                            t0 = time.perf_counter()
                            data = await asyncio.wait_for(fn(**args), timeout=remain)
                            ms = int((time.perf_counter() - t0) * 1000)
                            resp = {"req_id": rid, "code": OK, "err": "", "data": data, "ms": ms}
                            self._log(peer, rid, op, "SUCCESS", exec_ms=ms)
                            if op_id is not None:
                                payload = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
                                await self._db.execute(
                                    "INSERT OR IGNORE INTO op_log(op_id, result_json, created_at) VALUES (?, ?, ?)",
                                    (op_id, payload, int(time.time() * 1000))
                                )
                                await self._db.commit()
                                self._oplog[op_id] = data
                    else:
                        t0 = time.perf_counter()
                        data = await fn(**args)
                        ms = int((time.perf_counter() - t0) * 1000)
                        resp = {"req_id": rid, "code": OK, "err": "", "data": data, "ms": ms}
                        self._log(peer, rid, op, "SUCCESS", exec_ms=ms)
                        if op_id is not None:
                            payload = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
                            await self._db.execute(
                                "INSERT OR IGNORE INTO op_log(op_id, result_json, created_at) VALUES (?, ?, ?)",
                                (op_id, payload, int(time.time() * 1000))
                            )
                            await self._db.commit()

                            self._oplog[op_id] = data
                except asyncio.TimeoutError:
                    resp = {"req_id": rid, "code": E_TIMEOUT, "err": "TIMEOUT"}

                    self._log(peer, rid, op, "TIMEOUT", reason="handler_timeout")
                except Exception as e:

                    resp = {"req_id": rid, "code": E_INTERNAL, "err": str(e)}

                    self._log(peer, rid, op, "ERROR", exception=str(e)[:100])

                await write_frame(writer, resp)

        except asyncio.IncompleteReadError:
            print(f"[server] client disconnected: {peer}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def serve(self, host: str = "127.0.0.1", port: int = 8888) -> None:
        await self._init_storage()
        server = await asyncio.start_server(self._handle_conn, host, port)
        addr = ", ".join(str(s.getsockname()) for s in server.sockets)
        print(f"[server] RPC server listening on {addr}")
        async with server:
            await server.serve_forever()
