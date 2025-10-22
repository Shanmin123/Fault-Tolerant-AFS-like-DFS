import asyncio
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

from rpc.framing import write_frame, read_frame

# Return codes that the server uses; the client does not enforce them,
# but we mirror the values so callers can check "code == 0" easily.
OK = 0
E_TIMEOUT = 1001


class RPCClient:
    """
    Minimal, robust RPC client using length-prefixed JSON framing.

    Design choices:
      - One TCP connection per call (simple and reliable for coursework).
        This avoids connection lifecycle bugs and is good enough for demos.
      - Call-level timeout with asyncio.wait_for.
      - Basic retry with exponential backoff and round-robin across server list.

    If you later need higher throughput, you can implement a connection pool
    (keep one connection per target server, plus a background receive loop).
    """

    def __init__(
        self,
        servers: List[str],
        timeout: float = 3.0,
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
        self._timeout = timeout
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

    async def call(self, op: str, args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
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

        # Attempt (retries + 1) times at most.
        for attempt in range(self._retries + 1):
            host, port = self._choose_server()
            try:
                # Open a fresh connection for this attempt.
                reader, writer = await asyncio.open_connection(host, port)

                t0 = time.perf_counter()
                await write_frame(writer, req)

                # Read response with a timeout; the server replies one frame per request.
                resp = await asyncio.wait_for(read_frame(reader), timeout=self._timeout)
                latency_ms = int((time.perf_counter() - t0) * 1000)

                # Close proactively; we do not reuse the connection in this simple client.
                writer.close()

                # Attach latency for observability on the client side.
                if isinstance(resp, dict):
                    resp["latency_ms"] = latency_ms
                return resp

            except asyncio.TimeoutError:
                # Timed out waiting for response → retry if allowed.
                if attempt < self._retries:
                    await asyncio.sleep(self._backoff_base * (2 ** attempt))
                    continue
                # No more retries: synthesize a timeout response.
                return {"req_id": req_id, "code": E_TIMEOUT, "err": "TIMEOUT", "data": None}

            except Exception as e:
                # Any network/serialization error: retry if allowed.
                if attempt < self._retries:
                    await asyncio.sleep(self._backoff_base * (2 ** attempt))
                    continue
                # Final failure: still return a timeout-like shape for simplicity.
                return {"req_id": req_id, "code": E_TIMEOUT, "err": f"NETWORK_ERROR: {e}", "data": None}