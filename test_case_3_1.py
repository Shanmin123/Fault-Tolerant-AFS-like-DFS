import asyncio
import base64
import os
import socket
import sys
import time
from pathlib import Path
from typing import Optional

from AFS.rpc.client import RPCClient

HOST = "127.0.0.1"
PROJECT_ROOT = Path(__file__).resolve().parent
WORK_DIR = PROJECT_ROOT / ".case3.1"
TEST_DATA_FILE = PROJECT_ROOT / "tests/data/test10000.txt"
TARGET_PAYLOAD_BYTES = 20_000_000  # ~20 MB to keep read in-flight without timeouts
READ_CRASH_DELAY = 3  
SERVER_BOOT_DELAY = 1.0
CURRENT_PORT: Optional[int] = None


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((HOST, 0))
        return sock.getsockname()[1]


def _rpc_target() -> str:
    if CURRENT_PORT is None:
        raise RuntimeError("RPC port not initialized")
    return f"{HOST}:{CURRENT_PORT}"


async def start_server(port: int) -> asyncio.subprocess.Process:
    env = os.environ.copy()
    env["PYTHONPATH"] = f"{PROJECT_ROOT}{os.pathsep}{env.get('PYTHONPATH','')}".rstrip(os.pathsep)
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    cmd = [sys.executable, "-m", "AFS.run_afs_server", HOST, str(port)]
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


async def crash_server(proc: asyncio.subprocess.Process) -> None:
    if proc.returncode is not None:
        return
    proc.kill()
    await proc.wait()


async def create_fixture(path: str, payload: bytes) -> None:
    rpc = RPCClient(
        [_rpc_target()],
        retries=1,
        connect_timeout=0.5,
        write_timeout=10.0,
        read_timeout=10.0,
        deadline_sec=60.0,
    )
    resp = await rpc.call("Create", {"path": path})
    if resp["code"] != 0 and "File exists" not in resp.get("err", ""):
        raise RuntimeError(resp.get("err", "create failed"))
    b64 = base64.b64encode(payload).decode("ascii")
    write = await rpc.call("PutFile", {"path": path, "bytes": b64, "base_version": 1})
    if write["code"] != 0:
        raise RuntimeError(write.get("err", "put failed"))


async def read_once(path: str) -> bytes:
    rpc = RPCClient(
        [_rpc_target()],
        retries=0,
        connect_timeout=0.5,
        write_timeout=5.0,
        read_timeout=10.0,
        deadline_sec=60.0,
    )
    resp = await rpc.call("GetFile", {"path": path})
    if resp["code"] != 0:
        raise RuntimeError(resp.get("err", "read failed"))
    return base64.b64decode(resp["data"]["bytes"].encode("ascii"))


async def read_local_dataset() -> bytes:
    print(f"Reading test payload from {TEST_DATA_FILE} via cat...")
    proc = await asyncio.create_subprocess_exec(
        "cat",
        str(TEST_DATA_FILE),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        err = stderr.decode().strip() or "unknown error"
        raise RuntimeError(f"cat failed: {err}")
    sample = stdout[:200].decode("utf-8", errors="replace")
    print(f"Successfully read {len(stdout)} bytes from {TEST_DATA_FILE}")
    print("Original sample contents:\n" + sample + "\n")
    return stdout, sample


async def main() -> None:
    if WORK_DIR.exists():
        import shutil
        shutil.rmtree(WORK_DIR, ignore_errors=True)

    port = _pick_free_port()
    global CURRENT_PORT
    CURRENT_PORT = port
    proc = await start_server(port)
    await asyncio.sleep(SERVER_BOOT_DELAY)

    dataset, original_sample = await read_local_dataset()
    if not dataset:
        raise RuntimeError(f"{TEST_DATA_FILE} is empty")

    path = f"/tests/data/read_crash_{int(time.time())}.txt"
    repeats = max(1, (TARGET_PAYLOAD_BYTES + len(dataset) - 1) // len(dataset))
    payload = (dataset * repeats)[:TARGET_PAYLOAD_BYTES]
    print("Uploading fixture...")
    await create_fixture(path, payload)
    print("Fixture uploaded\n")

    print(f"Crash during read (waiting {READ_CRASH_DELAY:.1f}s before kill)...")
    reader = asyncio.create_task(read_once(path))
    waited = 0.0
    interval = 0.2
    while waited < READ_CRASH_DELAY:
        remaining = READ_CRASH_DELAY - waited
        await asyncio.sleep(min(interval, remaining))
        waited += min(interval, remaining)
        if reader.done():
            print("Read finished before crash delay; restarting read for demo...")
            reader = asyncio.create_task(read_once(path))
            await asyncio.sleep(0.01)
            break
        print(f"  ...{waited:.1f}s elapsed, read still running")
    if reader.done():
        raise AssertionError("Read completed before crash could be injected")
    print("Crashing server during read...")
    await crash_server(proc)

    try:
        await reader
    except Exception as exc:
        print(f"Read failed as expected: {exc}")
    else:
        raise AssertionError("Read unexpectedly succeeded during server crash")

    print("Restarting server...")
    proc = await start_server(port)
    await asyncio.sleep(SERVER_BOOT_DELAY)

    print("Retrying read after restart...")
    data = await read_once(path)
    if data != payload:
        raise AssertionError("Data mismatch after recovery")
    final_sample = data[:200].decode("utf-8", errors="replace")
    print("Recovered sample contents:\n" + final_sample)
    if final_sample != original_sample:
        raise AssertionError("Recovered sample differs from original input")
    print("Recovered sample matches original input.")
    print("Server crash during read handled; data intact.")

    await stop_server(proc)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user")
