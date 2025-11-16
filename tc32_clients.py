import argparse
import asyncio
import os

from AFS.afs.client import AFSClient
from AFS.rpc.client import RPCClient


def generate_payload(tag: str, lines: int) -> bytes:
    return "\n".join(f"{tag}-{i}" for i in range(lines)).encode("utf-8")


async def run_partial(host: str, port: int, path: str, tag: str, lines: int) -> None:
    payload = generate_payload(tag, lines)
    rpc = RPCClient([f"{host}:{port}"], retries=1)
    afs = AFSClient(rpc)
    try:
        fd = await afs.create(path)
    except Exception:
        fd = await afs.open(path, "w")
    await afs.write(fd, payload)
    print("[tc32_clients] crashing before close to simulate client failure")
    os._exit(1)


async def run_full(host: str, port: int, path: str, tag: str, lines: int) -> None:
    payload = generate_payload(tag, lines)
    rpc = RPCClient([f"{host}:{port}"], retries=2)
    afs = AFSClient(rpc)
    try:
        fd = await afs.open(path, "w")
    except Exception:
        fd = await afs.create(path)
    await afs.write(fd, payload)
    await afs.close(fd)
    print("[tc32_clients] write + close complete")


async def main(mode: str, host: str, port: int, path: str, tag: str, lines: int) -> None:
    if mode == "partial":
        await run_partial(host, port, path, tag, lines)
    else:
        await run_full(host, port, path, tag, lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Helper client used by tc32 manual flows.")
    parser.add_argument("--mode", choices=["partial", "full"], required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8888)
    parser.add_argument("--path", required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--lines", type=int, required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(main(args.mode, args.host, args.port, args.path, args.tag, args.lines))
