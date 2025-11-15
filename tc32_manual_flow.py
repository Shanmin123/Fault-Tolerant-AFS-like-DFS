import argparse
import asyncio
import time
import uuid

from tc32_clients import generate_payload
from tc32_workflow import (
    FINAL_LINES,
    FINAL_TAG,
    PARTIAL_LINES,
    PARTIAL_TAG,
    fetch_remote_bytes,
    unique_path,
)


def _partial_command(path: str, host: str, port: int, tag: str, lines: int) -> str:
    return (
        f"python -m tc32_clients --mode partial --host {host} --port {port} "
        f'--path "{path}" --tag {tag} --lines {lines}'
    )


def _full_command(path: str, host: str, port: int, tag: str, lines: int) -> str:
    return (
        f"python -m tc32_clients --mode full --host {host} --port {port} "
        f'--path "{path}" --tag {tag} --lines {lines}'
    )


async def manual_case32(
    path: str,
    host: str,
    port: int,
    partial_tag: str,
    partial_lines: int,
    final_tag: str,
    final_lines: int,
) -> None:
    print("[manual-3.2] Ensure AFS server is already running at the address below.")
    address = f"{host}:{port}"
    print(f"[manual-3.2] Target: {address}, file: {path}")

    partial_cmd = _partial_command(path, host, port, partial_tag, partial_lines)
    print("\nRun the following command in another terminal and allow it to crash before close:")
    print(partial_cmd)
    await asyncio.to_thread(
        input,
        ">>> After the partial client crashes (simulating client failure), press Enter here...",
    )
    # Wait until server is reachable again in case it was stopped
    after_crash = b""
    for _ in range(20):
        try:
            after_crash = await fetch_remote_bytes(path)
            break
        except RuntimeError as exc:
            if "CONNECTION_REFUSED" not in str(exc).upper():
                raise
            await asyncio.sleep(0.5)
    else:
        raise RuntimeError("Server did not become reachable after client crash; ensure it is running.")
    if after_crash:
        raise AssertionError("Server contains data after crash; expected empty file.")
    print("[manual-3.2] Verified server state is clean after client crash.\n")

    full_cmd = _full_command(path, host, port, final_tag, final_lines)
    print("Now run the healthy client to finish the write:")
    print(full_cmd)
    await asyncio.to_thread(
        input,
        ">>> After the healthy client finishes (no crash), press Enter here...",
    )
    await asyncio.sleep(0.5)
    expected = generate_payload(final_tag, final_lines)
    final_data = await fetch_remote_bytes(path)
    if final_data != expected:
        raise AssertionError("Final data mismatch; healthy client write failed.")
    print("[manual-3.2] Final data verified. Manual client-crash workflow complete.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Manual client-crash workflow for test_case_3.2 (user runs/crashes clients)."
    )
    parser.add_argument(
        "--path",
        default=unique_path("tc32_manual"),
        help="AFS file path to use for the demonstration.",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8888)
    parser.add_argument("--partial-tag", default=PARTIAL_TAG)
    parser.add_argument("--partial-lines", type=int, default=PARTIAL_LINES)
    parser.add_argument("--final-tag", default=FINAL_TAG)
    parser.add_argument("--final-lines", type=int, default=FINAL_LINES)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(
            manual_case32(
                path=args.path,
                host=args.host,
                port=args.port,
                partial_tag=args.partial_tag,
                partial_lines=args.partial_lines,
                final_tag=args.final_tag,
                final_lines=args.final_lines,
            )
        )
    except KeyboardInterrupt:
        print("\nInterrupted by user")


if __name__ == "__main__":
    main()
