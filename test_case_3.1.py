import asyncio
import base64
import os
import sys
import time
from pathlib import Path

from AFS.rpc.client import RPCClient

HOST = "127.0.0.1"
PORT = 8888
PROJECT_ROOT = Path(__file__).resolve().parent
WORK_DIR = PROJECT_ROOT / ".case3.1"


async def start_server() -> asyncio.subprocess.Process:
    env = os.environ.copy()
    env["PYTHONPATH"] = f"{PROJECT_ROOT}{os.pathsep}{env.get('PYTHONPATH','')}".rstrip(os.pathsep)
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, "-m", "AFS.run_afs_server", HOST, str(PORT)]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        cwd=str(WORK_DIR),
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    asyncio.create_task(pipe_logs(proc))
    return proc


async def pipe_logs(proc: asyncio.subprocess.Process) -> None:
    if not proc.stdout:
        return
    try:
        async for line in proc.stdout:
            text = line.decode(errors="ignore").rstrip()
            if text:
                print(f"[server] {text}")
    except asyncio.CancelledError:
        pass


async def stop_server(proc: asyncio.subprocess.Process) -> None:
    if proc.returncode is not None:
        return
    proc.terminate()
    try:
        await asyncio.wait_for(proc.wait(), timeout=5)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()


async def create_fixture(path: str, payload: bytes) -> None:
    rpc = RPCClient([f"{HOST}:{PORT}"], retries=1)
    resp = await rpc.call("Create", {"path": path})
    if resp["code"] != 0 and "File exists" not in resp.get("err", ""):
        raise RuntimeError(resp.get("err", "create failed"))
    b64 = base64.b64encode(payload).decode("ascii")
    write = await rpc.call("PutFile", {"path": path, "bytes": b64, "base_version": 1})
    if write["code"] != 0:
        raise RuntimeError(write.get("err", "put failed"))


async def read_once(path: str) -> bytes:
    rpc = RPCClient([f"{HOST}:{PORT}"], retries=0, read_timeout=5.0)
    resp = await rpc.call("GetFile", {"path": path})
    if resp["code"] != 0:
        raise RuntimeError(resp.get("err", "read failed"))
    return base64.b64decode(resp["data"]["bytes"].encode("ascii"))


async def main() -> None:
    if WORK_DIR.exists():
        import shutil
        shutil.rmtree(WORK_DIR, ignore_errors=True)

    proc = await start_server()
    await asyncio.sleep(0.5)

    path = f"/tests/data/read_crash_{int(time.time())}.txt"
    payload = b"x" * 5_000_000  # ~5 MB
    print("Uploading fixture...")
    await create_fixture(path, payload)
    print("Fixture uploaded\n")

    print("Crash during read ")
    reader = asyncio.create_task(read_once(path))
    await asyncio.sleep(0.05)
    print("Crashing server during read...")
    await stop_server(proc)

    try:
        await reader
        raise AssertionError("Read unexpectedly succeeded during server crash")
    except Exception as exc:
        print(f"Read failed as expected: {exc}")

    print("Restarting server...")
    proc = await start_server()
    await asyncio.sleep(0.5)

    print("Retrying read after restart...")
    data = await read_once(path)
    if data != payload:
        raise AssertionError("Data mismatch after recovery")
    print("Server crash during read handled; data intact.")

    await stop_server(proc)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user")
