"""
A minimal asynchronous RPC server with JSON framing.

This module defines a simple RPC server that listens for incoming TCP
connections and handles JSON-based RPC requests over a length-prefixed
framing. Each RPC request must include an operation identifier (`op`)
that maps to a registered handler function. Responses are JSON objects
that include metadata such as execution time and request correlation IDs.

Error handling is built into the server, ensuring that unexpected
exceptions in handler execution do not compromise the server loop.

Exported classes:
- RPCServer: A basic RPC server with support for registration of
  asynchronous request handlers and one-to-one request-response
  communication.

The architecture uses asyncio to provide concurrent handling of multiple
connections without additional threading or multiprocessing logic.
"""

import asyncio
import uuid
import time
from typing import Dict, Callable, Awaitable
from rpc.framing import read_frame, write_frame
import json
import aiosqlite

# --- Return codes (keep the set tiny and memorable) --------------------------
OK = 0               # success
E_INTERNAL = 1999    # server-side failure (unknown op, unhandled exception, etc.)
E_TIMEOUT = 1001
# A handler is an *async* function that accepts keyword arguments and returns a dict.
# Example:
#   async def Add(a: int, b: int) -> dict:
#       return {"result": a + b}
Handler = Callable[..., Awaitable[dict]]


class RPCServer:
    """
    A minimal RPC server over a length-prefixed JSON framing:
      - Each TCP connection is handled by one coroutine (asyncio).
      - For every incoming JSON request, we dispatch by 'op' (operation name).
      - We always respond with a JSON object that includes:
            { "req_id": <str>, "code": <int>, "err": <str>, "data": <dict>, "ms": <int> }
        where:
            req_id: request correlation id (echoed back for tracing)
            code:   0 on success, otherwise an error code
            err:    non-empty only when code != 0
            data:   handler-specific payload (dict)
            ms:     handler execution time in milliseconds
      - No connection reuse logic is needed on the server; asyncio handles concurrency.
    """

    def __init__(self) -> None:
        # Registry mapping 'op' (str) -> async handler
        self._handlers: Dict[str, Handler] = {}
        self._db_path = "oplog.db"          # persistent idempotency store (SQLite)
        self._db = None                     # will be set in _init_storage()
        # self._oplog remains as a fast in-memory cache; DB is the source of truth
        self._oplog: Dict[str, dict] = {}   # business idempotency: op_id -> last successful data

    def register(self, op: str, fn: Handler) -> None:
        """
        Register an RPC operation.
        The handler must be an async function that accepts keyword args (from request "args").
        """
        self._handlers[op] = fn

    async def _init_storage(self) -> None:
        # Open SQLite (async) and create the idempotency table
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

    async def _handle_conn(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """
        Per-connection loop:
          - Keep reading frames until the client closes the socket.
          - For each frame: parse, dispatch, write a response.
        """
        peer = writer.get_extra_info("peername")
        print(f"[server] client connected: {peer}")

        try:
            while True:
                # 1) Read one full JSON message using our framing (raises on EOF/short read).
                req = await read_frame(reader)

                # 2) Extract request fields with safe fallbacks.
                op = req.get("op")                              # operation name, e.g. "Echo"
                args = req.get("args", {}) or {}                # parameters as a dict
                rid = req.get("req_id", str(uuid.uuid4()))      # correlate request/response

                # BUSINESS IDEMPOTENCY (op_id): check persistent store first
                op_id = args.pop("op_id", None)
                if op_id is not None:
                    cur = await self._db.execute("SELECT result_json FROM op_log WHERE op_id = ?", (op_id,))
                    row = await cur.fetchone()
                    await cur.close()
                    if row:
                        prev = json.loads(row[0])
                        resp = {"req_id": rid, "code": OK, "err": "", "data": prev, "ms": 0}
                        await write_frame(writer, resp)
                        continue

                # NEW: per-request timing and optional client-provided deadline (in ms)
                t_req = time.perf_counter()
                deadline_ms = req.get("deadline_ms", None)

                # 3) Locate the handler. If not found, reply with an error immediately.
                fn = self._handlers.get(op)
                if fn is None:
                    resp = {"req_id": rid, "code": E_INTERNAL, "err": f"unknown op '{op}'"}
                    await write_frame(writer, resp)
                    continue

                # 4) Execute the handler safely; enforce remaining deadline if provided.
                try:
                    # Compute remaining time budget (seconds) if client supplied deadline_ms
                    if isinstance(deadline_ms, (int, float)):
                        remain = (deadline_ms / 1000.0) - (time.perf_counter() - t_req)
                        if remain <= 0:
                            resp = {"req_id": rid, "code": E_TIMEOUT, "err": "DEADLINE_EXCEEDED"}
                        else:
                            t0 = time.perf_counter()
                            data = await asyncio.wait_for(fn(**args), timeout=remain)
                            ms = int((time.perf_counter() - t0) * 1000)
                            resp = {"req_id": rid, "code": OK, "err": "", "data": data, "ms": ms}
                            # Persist business idempotency result (op_id-based)
                            if op_id is not None:
                                payload = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
                                await self._db.execute(
                                    "INSERT OR IGNORE INTO op_log(op_id, result_json, created_at) VALUES (?, ?, ?)",
                                    (op_id, payload, int(time.time() * 1000))
                                )
                                await self._db.commit()
                                # Optional hot cache
                                self._oplog[op_id] = data
                    else:
                        # No deadline provided; run normally
                        t0 = time.perf_counter()
                        data = await fn(**args)
                        ms = int((time.perf_counter() - t0) * 1000)
                        resp = {"req_id": rid, "code": OK, "err": "", "data": data, "ms": ms}
                        # Persist business idempotency result (op_id-based)
                        if op_id is not None:
                            payload = json.dumps(data, separators=(",", ":"), ensure_ascii=False)
                            await self._db.execute(
                                "INSERT OR IGNORE INTO op_log(op_id, result_json, created_at) VALUES (?, ?, ?)",
                                (op_id, payload, int(time.time() * 1000))
                            )
                            await self._db.commit()
                            # Optional hot cache
                            self._oplog[op_id] = data
                except asyncio.TimeoutError:
                    # Handler exceeded remaining deadline
                    resp = {"req_id": rid, "code": E_TIMEOUT, "err": "TIMEOUT"}
                except Exception as e:
                    # Map any unhandled exception to a generic INTERNAL error.
                    resp = {"req_id": rid, "code": E_INTERNAL, "err": str(e)}

                # 5) Send the response as one framed JSON message.
                await write_frame(writer, resp)

        except asyncio.IncompleteReadError:
            # The client closed the connection or sent a truncated frame.
            # This is normal shutdown behavior from the client's perspective.
            print(f"[server] client disconnected: {peer}")
        finally:
            # Ensure the writer is closed so the socket is released promptly.
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def serve(self, host: str = "127.0.0.1", port: int = 8888) -> None:
        """
        Start listening on (host, port) and serve forever.
        asyncio.start_server() will spawn _handle_conn for each incoming connection.
        """
        await self._init_storage()
        server = await asyncio.start_server(self._handle_conn, host, port)
        addr = ", ".join(str(s.getsockname()) for s in server.sockets)
        print(f"[server] RPC server listening on {addr}")
        async with server:
            await server.serve_forever()