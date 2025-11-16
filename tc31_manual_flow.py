import argparse
import asyncio
import base64
import time
import uuid
from pathlib import Path
from typing import Tuple

from AFS.rpc.client import RPCClient

PROJECT_ROOT = Path(__file__).resolve().parent


async def read_local_dataset(file_path: Path) -> Tuple[bytes, str]:
    data = file_path.read_bytes()
    sample = data[:200].decode("utf-8", errors="replace")
    print(f"Successfully read {len(data)} bytes from {file_path}")
    print("Original sample contents:\n" + sample + "\n")
    return data, sample


def build_payload(dataset: bytes, target_size: int) -> bytes:
    repeats = max(1, (target_size + len(dataset) - 1) // len(dataset)) if dataset else 1
    return (dataset * repeats)[:target_size]


async def upload_fixture(address: str, path: str, payload: bytes) -> None:
    rpc = RPCClient(
        [address],
        retries=1,
        connect_timeout=0.5,
        write_timeout=10.0,
        read_timeout=10.0,
        deadline_sec=60.0,
    )
    resp = await rpc.call("Create", {"path": path})
    base_version = 1
    if resp["code"] != 0:
        err = resp.get("err", "")
        if "file exists" in err.lower():
            open_resp = await rpc.call("Open", {"path": path})
            if open_resp["code"] != 0:
                raise RuntimeError(open_resp.get("err", "open failed"))
            base_version = int(open_resp["data"]["version"])
            print(f"[manual-3.1] File exists at {path}, reusing version {base_version}")
        else:
            raise RuntimeError(err or "create failed")
    b64 = base64.b64encode(payload).decode("ascii")
    write = await rpc.call("PutFile", {"path": path, "bytes": b64, "base_version": base_version})
    if write["code"] != 0:
        raise RuntimeError(write.get("err", "put failed"))


async def read_once(address: str, path: str) -> bytes:
    rpc = RPCClient(
        [address],
        retries=0,
        connect_timeout=0.5,
        write_timeout=5.0,
        read_timeout=10.0,
        deadline_sec=60.0,
    )
    resp = await rpc.call("GetFile", {"path": path})
    if resp["code"] != 0:
        raise RuntimeError(resp.get("err", "read failed"))
    payload = resp["data"].get("bytes")
    return base64.b64decode(payload.encode("ascii")) if payload else b""


async def manual_case31(
    path: str,
    address: str,
    dataset: Path,
    target_bytes: int,
) -> None:
    data, sample = await read_local_dataset(dataset)
    if not data:
        raise RuntimeError(f"{dataset} is empty")
    payload = build_payload(data, target_bytes)
    print(f"[manual-3.1] Uploading payload to {path} via {address}")
    await upload_fixture(address, path, payload)
    print("[manual-3.1] Fixture stored. Crash the server whenever you are ready.")

    await asyncio.to_thread(
        input,
        ">>> Kill the AFS server (Ctrl+C/close terminal). "
        "Press Enter here once it is down (if auto-kill is armed, wait for it to run)...",
    )

    await asyncio.to_thread(
        input,
        ">>> Restart the AFS server (same host/port) and press Enter once it is accepting RPCs...",
    )
    await asyncio.sleep(1.0)
    data_after = await read_once(address, path)
    if data_after != payload:
        raise AssertionError("Recovered data mismatch")
    first = data_after[:200].decode("utf-8", errors="replace")
    print("[manual-3.1] Recovery successful. Sample contents:\n" + first)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Manual server-crash workflow for test_case_3.1 (user controls the server process)."
    )
    parser.add_argument(
        "--path",
        default=f"/tests/data/tc31_manual_{int(time.time())}_{uuid.uuid4().hex}.txt",
        help="AFS path to use for the manual demonstration.",
    )
    parser.add_argument("--address", default="127.0.0.1:8888", help="Address of the manually started server.")
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("tests/data/test10000.txt"),
        help="Local dataset used to build the payload.",
    )
    parser.add_argument("--target-bytes", type=int, default=20_000_000, help="Target payload size.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(manual_case31(args.path, args.address, args.dataset, args.target_bytes))
    except KeyboardInterrupt:
        print("\nInterrupted by user")


if __name__ == "__main__":
    main()
