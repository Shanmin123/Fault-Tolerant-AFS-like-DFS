import asyncio
import base64
import os
import shutil
import sys
import time
import uuid
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from AFS.rpc.client import RPCClient

HOST = "127.0.0.1"
SERVER_IDS = ("server1", "server2", "server3")
PORTS = {"server1": 8888, "server2": 8889, "server3": 8890}
PROJECT_ROOT = Path(__file__).resolve().parent
RUNTIME_ROOT = PROJECT_ROOT / ".replication_runtime"

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


class Cluster:
    def __init__(self) -> None:
        self.root = RUNTIME_ROOT
        self.nodes: Dict[str, Dict] = {
            sid: {
                "address": f"{HOST}:{PORTS[sid]}",
                "peers": [f"{HOST}:{PORTS[p]}" for p in SERVER_IDS if p != sid],
                "workdir": self.root / sid,
                "proc": None,
                "logs": None,
            }
            for sid in SERVER_IDS
        }

    async def start(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root)
        for idx, sid in enumerate(SERVER_IDS):
            await self._launch(sid)
            await asyncio.sleep(0.2 if idx == 0 else 0.4)
        await asyncio.sleep(1.0)

    async def stop_all(self) -> None:
        for sid in SERVER_IDS:
            await self._stop(sid)

    async def restart(self, node_id: str) -> None:
        await self._stop(node_id, graceful=False)
        await self._launch(node_id)
        await asyncio.sleep(1.0)

    async def kill_by_address(self, address: str) -> Optional[str]:
        for sid, spec in self.nodes.items():
            if spec["address"] == address:
                await self._stop(sid, graceful=False)
                return sid
        return None

    def addresses(self, alive_only: bool = True) -> List[str]:
        result = []
        for spec in self.nodes.values():
            proc = spec["proc"]
            if proc is None or (alive_only and proc.returncode is not None):
                continue
            result.append(spec["address"])
        if alive_only and not result:
            return [spec["address"] for spec in self.nodes.values()]
        return result

    async def _launch(self, sid: str) -> None:
        spec = self.nodes[sid]
        spec["workdir"].mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        existing = env.get("PYTHONPATH")
        env["PYTHONPATH"] = f"{PROJECT_ROOT}{os.pathsep}{existing}" if existing else str(PROJECT_ROOT)
        cmd = [
            sys.executable,
            "-m",
            "AFS.raft.server",
            sid,
            HOST,
            str(PORTS[sid]),
            *spec["peers"],
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=str(spec["workdir"]),
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        spec["proc"] = proc
        spec["logs"] = asyncio.create_task(self._pipe_logs(sid, proc.stdout))

    async def _stop(self, sid: str, graceful: bool = True) -> None:
        spec = self.nodes[sid]
        proc = spec["proc"]
        if not proc or proc.returncode is not None:
            return
        (proc.terminate if graceful else proc.kill)()
        try:
            await asyncio.wait_for(proc.wait(), timeout=5)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
        if spec["logs"]:
            spec["logs"].cancel()
            spec["logs"] = None
        spec["proc"] = None

    async def _pipe_logs(self, sid: str, stream: Optional[asyncio.StreamReader]) -> None:
        if not stream:
            return
        try:
            async for line in stream:
                text = line.decode(errors="ignore").rstrip()
                if text:
                    print(f"[{sid}] {text}")
        except asyncio.CancelledError:
            pass


class FailoverRPC:
    def __init__(self, cluster: Cluster) -> None:
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
            print(f"[test] transient RPC failure on {target}: {last_error}, retrying...")
            await asyncio.sleep(0.3)
        raise RuntimeError(last_error or f"{op} failed")

    async def create(self, path: str) -> Tuple[int, str]:
        resp, leader = await self.call("Create", {"path": path}, use_op_id=True)
        return int(resp["data"]["version"]), leader

    async def put(self, path: str, payload: bytes, base_version: int, prefer: Optional[str] = None) -> Tuple[int, str]:
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
        data = base64.b64decode(payload.get("bytes", "").encode("ascii")) if payload.get("bytes") else b""
        return int(payload.get("version", 0)), data, server

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
    return base64.b64decode(payload.get("bytes", "").encode("ascii")) if payload.get("bytes") else b""


async def wait_for_consistency(addresses: Iterable[str], path: str, expected: bytes, timeout: float = 10.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
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
        if asyncio.get_event_loop().time() > deadline:
            raise AssertionError(f"Consistency check failed: {last_error}")
        await asyncio.sleep(0.5)


async def put_with_retry(rpc: FailoverRPC, path: str, payload: bytes, base_version: int, prefer: Optional[str] = None) -> Tuple[int, str]:
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
        if "file exists" not in str(exc).lower():
            raise
        print(f"[test] File already exists for {path}, reusing it")
        version, leader = await rpc.open_version(path)
    return await put_with_retry(rpc, path, payload, version, prefer=leader)


async def failover_write(
    cluster: Cluster,
    rpc: FailoverRPC,
    path: str,
    base_version: int,
    leader: str,
    payload: bytes,
) -> Tuple[int, str, str]:
    write_task = asyncio.create_task(put_with_retry(rpc, path, payload, base_version, prefer=leader))
    await asyncio.sleep(0.05)
    failed_node = await cluster.kill_by_address(leader)
    if not failed_node:
        raise RuntimeError(f"Failed to kill leader at {leader}")
    new_version, new_leader = await write_task
    return new_version, new_leader, failed_node


async def run_test() -> None:
    cluster = Cluster()
    await cluster.start()
    rpc = FailoverRPC(cluster)
    path = f"/tests/outputs/test_case_3_{int(time.time())}_{uuid.uuid4().hex}.txt"
    base_payload = make_payload("replication-initial", 64)
    failover_payload = make_payload("replication-failover", 60000)

    try:
        print("\nPhase 1: baseline replication ")
        version, leader = await bootstrap_file(rpc, path, base_payload)
        await wait_for_consistency(cluster.addresses(True), path, base_payload)
        print("Baseline replication verified on all servers\n")

        print("Phase 2: kill leader during write ")
        version, new_leader, failed_node = await failover_write(cluster, rpc, path, version, leader, failover_payload)
        await wait_for_consistency(cluster.addresses(True), path, failover_payload)
        print(f"Write completed via {new_leader} after {failed_node} crashed\n")

        print("Phase 3: recovery and catch-up ")
        await cluster.restart(failed_node)
        await wait_for_consistency(cluster.addresses(False), path, failover_payload)
        print("Recovered server is consistent with existing replicas\n")
        print(f"File under test: {path}")
    finally:
        await cluster.stop_all()


def main() -> None:
    try:
        asyncio.run(run_test())
    except KeyboardInterrupt:
        print("\nInterrupted by user")


if __name__ == "__main__":
    main()
