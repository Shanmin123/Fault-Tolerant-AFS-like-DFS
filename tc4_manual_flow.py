import argparse
import asyncio
import base64
import time
import uuid
from typing import Dict, Iterable, List, Optional, Tuple

from AFS.rpc.client import RPCClient


class ManualCluster:
    """Simple adapter that exposes a static list of server addresses."""

    def __init__(self, addresses: Iterable[str]) -> None:
        self._addresses = list(addresses)

    def addresses(self, alive_only: bool = True) -> List[str]:
        return list(self._addresses)


TRANSIENT_KEYWORDS = ("connection", "timeout", "refused", "reset", "unreachable", "unavailable")


class VersionConflictError(RuntimeError):
    pass


async def call_rpc_once(address: str, op: str, args: Dict, *, op_id: Optional[str] = None) -> Dict:
    rpc = RPCClient(
        [address],
        connect_timeout=0.5,
        read_timeout=2.0,
        write_timeout=1.0,
        deadline_sec=3.0,
        retries=0,
    )
    return await rpc.call(op, args, op_id=op_id)


class FailoverRPC:
    def __init__(self, cluster: ManualCluster) -> None:
        self.cluster = cluster

    async def call(
        self,
        op: str,
        args: Dict,
        *,
        prefer: Optional[str] = None,
        attempts: int = 20,
        use_op_id: bool = False,
    ) -> Tuple[Dict, str]:
        op_token = f"{op}-{uuid.uuid4()}" if use_op_id else None
        addresses = self._ordered_addresses(prefer)
        if not addresses:
            raise RuntimeError("No servers available")
        last_error = ""
        for attempt in range(attempts):
            target = addresses[attempt % len(addresses)]
            resp = await call_rpc_once(target, op, dict(args), op_id=op_token)
            if resp["code"] == 0:
                data = resp.get("data") or {}
                if data.get("error") == "not_leader":
                    hint = data.get("leader_hint")
                    if hint and hint not in addresses:
                        addresses.insert(0, hint)
                    await asyncio.sleep(0.1)
                    continue
                if data.get("error"):
                    last_error = data["error"]
                    await asyncio.sleep(0.2)
                    continue
                return resp, target
            last_error = resp.get("err", "")
            err_lower = last_error.lower()
            if "version conflict" in err_lower:
                raise VersionConflictError(last_error or "version conflict")
            if any(keyword in err_lower for keyword in TRANSIENT_KEYWORDS):
                await asyncio.sleep(0.2)
                continue
            print(f"[manual] transient RPC failure on {target}: {last_error}, retrying...")
            await asyncio.sleep(0.3)
        raise RuntimeError(last_error or f"{op} failed")

    async def create(self, path: str) -> Tuple[int, str]:
        resp, leader = await self.call("Create", {"path": path}, use_op_id=True)
        return int(resp["data"]["version"]), leader

    async def put(
        self, path: str, payload: bytes, base_version: int, prefer: Optional[str] = None
    ) -> Tuple[int, str]:
        resp, leader = await self.call(
            "PutFile",
            {
                "path": path,
                "bytes": base64.b64encode(payload).decode("ascii"),
                "base_version": base_version,
            },
            prefer=prefer,
            use_op_id=True,
        )
        return int(resp["data"]["new_version"]), leader

    async def open_version(self, path: str) -> Tuple[int, str]:
        resp, server = await self.call("Open", {"path": path})
        return int(resp["data"]["version"]), server

    async def snapshot(self, path: str) -> Tuple[int, bytes, str]:
        resp, server = await self.call("GetFile", {"path": path})
        payload = resp.get("data", {})
        data = payload.get("bytes")
        decoded = base64.b64decode(data.encode("ascii")) if data else b""
        return int(payload.get("version", 0)), decoded, server

    def _ordered_addresses(self, prefer: Optional[str]) -> List[str]:
        alive = self.cluster.addresses(True)
        alive = alive or self.cluster.addresses(False)
        if prefer and prefer in alive:
            alive.remove(prefer)
            alive.insert(0, prefer)
        return alive


def make_payload(tag: str, lines: int) -> bytes:
    return "\n".join(f"{tag}-{i}" for i in range(lines)).encode("utf-8")


async def fetch_from(address: str, path: str) -> bytes:
    resp = await call_rpc_once(address, "GetFile", {"path": path})
    if resp["code"] != 0:
        raise RuntimeError(resp.get("err", "GetFile failed"))
    payload = resp.get("data", {})
    data = payload.get("bytes")
    return base64.b64decode(data.encode("ascii")) if data else b""


async def wait_for_consistency(addresses: Iterable[str], path: str, expected: bytes, timeout: float = 10.0) -> None:
    loop = asyncio.get_event_loop()
    deadline = loop.time() + timeout
    last_error = ""
    while True:
        mismatch = False
        for addr in addresses:
            try:
                data = await fetch_from(addr, path)
                if data != expected:
                    mismatch = True
                    last_error = f"{addr} mismatch"
                    break
            except Exception as exc:
                mismatch = True
                last_error = f"{addr}: {exc}"
                break
        if not mismatch:
            return
        if loop.time() > deadline:
            raise AssertionError(f"Consistency check failed: {last_error}")
        await asyncio.sleep(0.5)


async def put_with_retry(
    rpc: FailoverRPC, path: str, payload: bytes, base_version: int, prefer: Optional[str] = None
) -> Tuple[int, str]:
    current_version = base_version
    hint = prefer
    for _ in range(6):
        try:
            return await rpc.put(path, payload, current_version, prefer=hint)
        except VersionConflictError:
            version, data, hint = await rpc.snapshot(path)
            if data == payload:
                return version, hint
            current_version = version
    raise RuntimeError("PutFile retries exceeded")


async def bootstrap_file(rpc: FailoverRPC, path: str, payload: bytes) -> Tuple[int, str]:
    try:
        version, leader = await rpc.create(path)
    except RuntimeError as exc:
        # When file already exists, fall back to opening the current version.
        print(f"[manual] Create failed for {path} ({exc}), attempting to reuse existing file")
        version, leader = await rpc.open_version(path)
    return await put_with_retry(rpc, path, payload, version, prefer=leader)


async def manual_failover(
    path: str,
    addresses: List[str],
    base_lines: int,
    failover_lines: int,
) -> None:
    cluster = ManualCluster(addresses)
    rpc = FailoverRPC(cluster)
    base_payload = make_payload("replication-initial", base_lines)
    failover_payload = make_payload("replication-failover", failover_lines)

    version, leader = await bootstrap_file(rpc, path, base_payload)
    await wait_for_consistency(addresses, path, base_payload)
    print(f"[manual] baseline replicated via {leader} for {path}")

    print(
        f"[manual] starting large write via leader {leader}. "
        "Kill THIS leader's process (close the matching terminal) while the write is running, "
        "then press Enter here once it is down."
    )
    write_task = asyncio.create_task(put_with_retry(rpc, path, failover_payload, version, prefer=leader))
    await asyncio.sleep(0.2)
    await asyncio.to_thread(input, ">>> Kill the leader now, then press Enter to continue...")
    new_version, new_leader = await write_task
    print(f"[manual] write finished with new leader {new_leader}, version {new_version}")

    alive_after = [addr for addr in addresses if addr != leader] or addresses
    await wait_for_consistency(alive_after, path, failover_payload)
    print("[manual] surviving replicas agree on failover payload")

    await asyncio.to_thread(
        input,
        f">>> Restart the crashed server ({leader}) and press Enter once it is back online...",
    )
    await wait_for_consistency(addresses, path, failover_payload)
    print("[manual] recovered replica caught up. Manual failover demo complete.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Manual failover workflow for test_case_4 with user-managed Raft servers."
    )
    parser.add_argument(
        "--path",
        default=f"/tests/outputs/tc4_manual_{int(time.time())}_{uuid.uuid4().hex}.txt",
        help="AFS file path used for the manual demonstration.",
    )
    parser.add_argument(
        "--addresses",
        nargs="+",
        default=["127.0.0.1:8888", "127.0.0.1:8889", "127.0.0.1:8890"],
        help="Addresses of the already-running Raft servers.",
    )
    parser.add_argument("--base-lines", type=int, default=64, help="Baseline payload size before failover.")
    parser.add_argument("--failover-lines", type=int, default=60000, help="Payload size used during failover write.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(manual_failover(args.path, args.addresses, args.base_lines, args.failover_lines))
    except KeyboardInterrupt:
        print("\nInterrupted by user")


if __name__ == "__main__":
    main()
