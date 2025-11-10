import asyncio
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from AFS.rpc.framing import write_frame, read_frame

OK = 0
E_TIMEOUT = 1001
E_CONNECT_FAILED = 1002
E_WRITE_FAILED = 1003
E_READ_FAILED = 1004


class RPCClient:

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

        if not servers:
            raise ValueError("servers list must not be empty")
        self._servers = servers
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        self._write_timeout = write_timeout
        self._deadline_sec = deadline_sec

        self._retries = retries
        self._backoff_base = backoff_base

        self._rr = 0

    def _choose_server(self) -> Tuple[str, int]:
        target = self._servers[self._rr % len(self._servers)]
        self._rr += 1
        host, port_s = target.split(":")
        return host, int(port_s)

    async def call(self, op: str, args: Optional[Dict[str, Any]] = None, *, op_id: Optional[str] = None) -> Dict[
        str, Any]:

        req_id = str(uuid.uuid4())
        req = {"op": op, "req_id": req_id, "args": (args or {})}

        if op_id is not None:
            _args = dict(req["args"])
            _args.setdefault("op_id", op_id)
            req["args"] = _args

        if self._deadline_sec is not None:
            req["deadline_ms"] = int(self._deadline_sec * 1000)

        last_error_code = E_TIMEOUT
        last_error_msg = "TIMEOUT"

        for attempt in range(self._retries + 1):
            host, port = self._choose_server()

            try:
                reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port),
                                                        timeout=self._connect_timeout)
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

                # PHASE 2: WRITE REQUEST
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

                try:
                    resp = await asyncio.wait_for(read_frame(reader), timeout=self._read_timeout)
                except asyncio.TimeoutError:
                    last_error_code = E_READ_FAILED
                    last_error_msg = f"READ_TIMEOUT from {host}:{port}"
                    raise
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
