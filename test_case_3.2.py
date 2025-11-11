import asyncio
import base64
import os
import shutil
import sys
import time
from pathlib import Path
from typing import Optional
from AFS.afs.client import AFSClient
from AFS.rpc.client import RPCClient
HOST = "127.0.0.1"
PORT = 8888
PROJECT_ROOT = Path(__file__).resolve().parent
WORK_DIR = PROJECT_ROOT / ".case3_2"
def generate_payload(tag: str, lines: int) -> bytes:
    return "\n".join(f"{tag}-{i}" for i in range(lines)).encode("utf-8")
async def start_server() -> asyncio.subprocess.Process:
    env = os.environ.copy()
    env["PYTHONPATH"] = f"{PROJECT_ROOT}{os.pathsep}{env.get('PYTHONPATH', '')}".rstrip(os.pathsep)
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
async def stop_server(proc: Optional[asyncio.subprocess.Process]) -> None:
    if not proc or proc.returncode is not None:
        return
    proc.terminate()
    try:
        await asyncio.wait_for(proc.wait(), timeout=5)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
async def run_partial_client(path: str, tag: str, lines: int) -> None:
    payload = generate_payload(tag, lines)
    rpc = RPCClient([f"{HOST}:{PORT}"], retries=1)
    afs = AFSClient(rpc)
    try:
        fd = await afs.create(path)
    except Exception:
        fd = await afs.open(path, "w")
    await afs.write(fd, payload)
    print("[partial-client] crashing before close")
    os._exit(1)
async def run_full_client(path: str, tag: str, lines: int) -> None:
    payload = generate_payload(tag, lines)
    rpc = RPCClient([f"{HOST}:{PORT}"], retries=2)
    afs = AFSClient(rpc)
    try:
        fd = await afs.open(path, "w")
    except Exception:
        fd = await afs.create(path)
    await afs.write(fd, payload)
    await afs.close(fd)
    print("[full-client] write + close complete")
async def fetch_remote_bytes(path: str) -> bytes:
    rpc = RPCClient([f"{HOST}:{PORT}"], retries=2)
    resp = await rpc.call("GetFile", {"path": path})
    if resp["code"] != 0:
        raise RuntimeError(resp.get("err", "GetFile failed"))
    return base64.b64decode(resp["data"]["bytes"].encode("ascii"))
async def run_subprocess(mode: str, path: str, tag: str, lines: int) -> int:
    env = os.environ.copy()
    env.update(
        {
            "CASE32_MODE": mode,
            "CASE32_PATH": path,
            "CASE32_TAG": tag,
            "CASE32_LINES": str(lines),
        }
    )
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        __file__,
        env=env,
    )
    return await proc.wait()
async def orchestrate() -> None:
    shutil.rmtree("srv_data", ignore_errors=True)
    shutil.rmtree(WORK_DIR, ignore_errors=True)
    server = await start_server()
    await asyncio.sleep(0.5)
    path = f"/tests/data/write_crash_{int(time.time())}.txt"
    partial_tag, partial_lines = "partial", 2000
    final_tag, final_lines = "final", 4000
    try:
        print("Simulating client crash during write...")
        code = await run_subprocess("partial", path, partial_tag, partial_lines)
        print(f"Partial client exited with code {code}")
        data_after_crash = await fetch_remote_bytes(path)
        if data_after_crash != b"":
            raise AssertionError("Server contains partial data after crash")
        print("Server verified: no partial data persisted\n")
        print("Running healthy client to write final output...")
        code = await run_subprocess("full", path, final_tag, final_lines)
        if code != 0:
            raise RuntimeError(f"Full client failed with exit code {code}")
        final_expected = generate_payload(final_tag, final_lines)
        final_data = await fetch_remote_bytes(path)
        if final_data != final_expected:
            raise AssertionError("Final data mismatch on server")
        print("Final data verified. Test passed.")
    finally:
        await stop_server(server)
async def main() -> None:
    mode = os.environ.get("CASE32_MODE", "orchestrate")
    path = os.environ.get("CASE32_PATH", "")
    tag = os.environ.get("CASE32_TAG", "")
    lines = int(os.environ.get("CASE32_LINES", "0"))
    if mode == "partial":
        await run_partial_client(path, tag, lines)
    elif mode == "full":
        await run_full_client(path, tag, lines)
    else:
        await orchestrate()
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nInterrupted by user")
