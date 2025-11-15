import argparse
import asyncio
import os
import signal
import time
import uuid
from contextlib import suppress
from pathlib import Path
from typing import Optional

from tc31_workflow import build_payload, read_local_dataset, read_once, upload_fixture


async def _kill_after_delay(pid: int, delay: float) -> None:
    await asyncio.sleep(max(0.0, delay))
    try:
        os.kill(pid, signal.SIGKILL)
        print(f"[manual-3.1] Auto-kill sent SIGKILL to PID {pid}")
    except ProcessLookupError:
        print(f"[manual-3.1] Auto-kill skipped; PID {pid} not found")
    except PermissionError:
        print(f"[manual-3.1] Auto-kill failed; insufficient permissions for PID {pid}")


async def manual_case31(
    path: str,
    address: str,
    dataset: Path,
    target_bytes: int,
    auto_kill_pid: Optional[int],
    auto_kill_delay: float,
) -> None:
    data, sample = await read_local_dataset(dataset)
    if not data:
        raise RuntimeError(f"{dataset} is empty")
    payload = build_payload(data, target_bytes)
    print(f"[manual-3.1] Uploading payload to {path} via {address}")
    await upload_fixture(address, path, payload)
    print("[manual-3.1] Fixture stored. Starting read; crash the server when prompted.")

    while True:
        reader = asyncio.create_task(read_once(address, path))
        start = time.time()
        await asyncio.to_thread(
            input,
            ">>> Press Enter when you are ready, then immediately kill the server...",
        )
        if reader.done():
            elapsed = time.time() - start
            print(f"[manual-3.1] Read already finished (took {elapsed:.1f}s). Retrying with a new read...")
            await asyncio.sleep(0.2)
            continue

        print(">>> Kill the server NOW (Ctrl+C). Waiting for crash...")
        kill_task: Optional[asyncio.Task] = None
        if auto_kill_pid is not None:
            kill_task = asyncio.create_task(_kill_after_delay(auto_kill_pid, auto_kill_delay))
        force_killed = False
        while True:
            if reader.done():
                try:
                    await reader
                except Exception as exc:
                    print(f"[manual-3.1] Read failed as expected due to crash: {exc}")
                    force_killed = True
                    break
                else:
                    print("[manual-3.1] Read still succeeded; restarting read...")
                    await asyncio.sleep(0.2)
                    break
                finally:
                    if kill_task:
                        kill_task.cancel()
                        with suppress(asyncio.CancelledError):
                            await kill_task
            await asyncio.sleep(0.1)
        if force_killed:
            break

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
    parser.add_argument(
        "--auto-kill-pid",
        type=int,
        default=None,
        help="PID of the server process to automatically SIGKILL after the prompt (optional).",
    )
    parser.add_argument(
        "--auto-kill-delay",
        type=float,
        default=2.0,
        help="Delay in seconds before auto-killing the PID (requires --auto-kill-pid).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(
            manual_case31(
                args.path,
                args.address,
                args.dataset,
                args.target_bytes,
                args.auto_kill_pid,
                args.auto_kill_delay,
            )
        )
    except KeyboardInterrupt:
        print("\nInterrupted by user")


if __name__ == "__main__":
    main()
