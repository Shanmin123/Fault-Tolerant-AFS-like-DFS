"""
A robust RPC client implementation using length-prefixed JSON framing.

This module provides an RPCClient class to perform remote procedure calls
over TCP connections. It is designed to be minimal and robust, featuring
mechanisms like per-call timeouts, exponential backoff retries, and round-robin
server selection. Each server call uses a unique TCP connection, avoiding
connection lifecycle issues and simplifying its usage.

Call responses from the server are expected to be in a structured JSON format,
and include attributes like latency for observability. In case of failures,
synthetic responses are returned for better client-side handling.
"""
import asyncio
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from rpc.framing import write_frame, read_frame

# Return codes that the server uses;
OK = 0
E_TIMEOUT = 1001
E_CONNECT_FAILED = 1002
E_WRITE_FAILED = 1003
E_READ_FAILED = 1004

class RPCClient:
    """
    Minimal, robust RPC client using length-prefixed JSON framing.

    Design:
      - One TCP connection per call
        This avoids connection lifecycle bugs and is good enough for demos.
      - Call-level timeout with asyncio.wait_for.
      - Basic retry with exponential backoff and round-robin across server list.

    """

    def __init__(
        self,
        servers: List[str],
        connect_timeout: float = 0.20,
        read_timeout: float = 0.60,
        write_timeout: float = 0.10,
        deadline_sec: Optional[float] = 1.80,

        retries: int = 1,
        backoff_base: float = 0.2,
    ) -> None:
        """
        Args:
            servers: list like ["127.0.0.1:8888", "10.0.0.2:8888"].
            timeout: seconds; per-call read timeout for the response.
            retries: how many extra attempts after the first try (total tries = retries + 1).
            backoff_base: base seconds for exponential backoff (t, 2t, 4t, ...).
        """
        if not servers:
            raise ValueError("servers list must not be empty")
        self._servers = servers
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        self._write_timeout = write_timeout
        self._deadline_sec = deadline_sec


        self._retries = retries
        self._backoff_base = backoff_base

        # Round-robin index across servers on each attempt.
        self._rr = 0

    def _choose_server(self) -> Tuple[str, int]:
        """
        Round-robin selection: rotate through the provided endpoints.
        Returns (host, port) tuple.
        """
        target = self._servers[self._rr % len(self._servers)]
        self._rr += 1
        host, port_s = target.split(":")
        return host, int(port_s)

    async def call(self, op: str, args: Optional[Dict[str, Any]] = None, *, op_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Perform a single RPC call:
          - Build request JSON {"op","req_id","args"}.
          - Pick a server (round-robin).
          - Open a TCP connection, write one frame, read one framed response.
          - On timeout or network failure, retry with exponential backoff.

        Returns:
            A dict that mirrors server response, e.g.:
            {"req_id": "...", "code": 0, "err": "", "data": {...}, "latency_ms": 12}

        On final failure (after all retries), returns a synthetic timeout response:
            {"req_id": "...", "code": 1001, "err": "TIMEOUT", "data": None}
        """
        req_id = str(uuid.uuid4())
        req = {"op": op, "req_id": req_id, "args": (args or {})}

        # attach user-supplied op_id so the server can de-duplicate
        if op_id is not None:
            _args = dict(req["args"])
            _args.setdefault("op_id", op_id)
            req["args"] = _args

        #  Fault Tolerance: pass end-to-end deadline to the server for adaptive timeout/cancellation
        if self._deadline_sec is not None:
            req["deadline_ms"] = int(self._deadline_sec * 1000)

        last_error_code = E_TIMEOUT
        last_error_msg = "TIMEOUT"

        # Attempt (retries + 1) times at most.
        for attempt in range(self._retries + 1):
            host, port = self._choose_server()

            #PHASE 1: CONNECT
            try:
                # Open a fresh connection for this attempt.
                reader, writer = await asyncio.wait_for( asyncio.open_connection(host, port),
                                       timeout = self._connect_timeout)
            except asyncio.TimeoutError:
                last_error_code = E_CONNECT_FAILED
                last_error_msg = f"CONNECTION_TIMEOUT to {host}:{port}"
                if attempt < self._retries:
                    await asyncio.sleep(self._backoff_base * (2 ** attempt))
                    continue
                return {"req_id": req_id, "code": last_error_code, "err": last_error_msg, "data": None}
            except ConnectionRefusedError:
                last_error_code = E_CONNECT_FAILED
                last_error_msg = f"CONNECTION_REFUSED by {host}:{port}"
                if attempt < self._retries:
                    await asyncio.sleep(self._backoff_base * (2 ** attempt))
                    continue
                return {"req_id": req_id, "code": last_error_code, "err": last_error_msg, "data": None}
            except OSError as e:
                last_error_code = E_CONNECT_FAILED
                last_error_msg = f"CONNECTION_ERROR to {host}:{port}: {e}"
                if attempt < self._retries:
                    await asyncio.sleep(self._backoff_base * (2 ** attempt))
                    continue
                return {"req_id": req_id, "code": last_error_code, "err": last_error_msg, "data": None}

            try:
                t0 = time.perf_counter()

                #PHASE 2: WRITE REQUEST
                try:
                    await asyncio.wait_for(write_frame(writer, req), timeout=self._write_timeout)
                except asyncio.TimeoutError:
                    last_error_code = E_WRITE_FAILED
                    last_error_msg = f"WRITE_TIMEOUT to {host}:{port}"
                    raise  # Jump to outer exception handler for retry
                except Exception as e:
                    last_error_code = E_WRITE_FAILED
                    last_error_msg = f"WRITE_ERROR to {host}:{port}: {e}"
                    raise

                #PHASE 3: READ RESPONSE
                try:
                    resp = await asyncio.wait_for(read_frame(reader), timeout=self._read_timeout)
                except asyncio.TimeoutError:
                    last_error_code = E_READ_FAILED
                    last_error_msg = f"READ_TIMEOUT from {host}:{port}"
                    raise  # Jump to outer exception handler for retry
                except Exception as e:
                    last_error_code = E_READ_FAILED
                    last_error_msg = f"READ_ERROR from {host}:{port}: {e}"
                    raise

                latency_ms = int((time.perf_counter() - t0) * 1000)
                if isinstance(resp, dict):
                    resp["latency_ms"] = latency_ms
                return resp
            except (asyncio.TimeoutError, Exception):
                if attempt < self._retries:
                    await asyncio.sleep(self._backoff_base * (2 ** attempt))
                    continue
                return {"req_id": req_id, "code": last_error_code, "err": last_error_msg, "data": None}
            
            finally:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass

        return {"req_id": req_id, "code": last_error_code, "err": last_error_msg, "data": None}