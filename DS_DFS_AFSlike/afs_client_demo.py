# afs_client_demo.py
import asyncio
from pathlib import Path
from DS_DFS_AFSlike.rpc.client import RPCClient
from DS_DFS_AFSlike.afs.client import AFSClient

async def main():
    rpc = RPCClient(["127.0.0.1:8888"])
    afs = AFSClient(rpc)

    path = "/demo.txt"
    print("=== Step 1: open_sync_read ===")
    s1 = await afs.open_sync_read(path)
    print(s1)

    # local cache file path
    local_file = afs.cache.fs_path(path)

    print("=== Step 2: edit local file ===")
    local_file.parent.mkdir(parents=True, exist_ok=True)
    local_file.write_text("Hello from local cache!\n", encoding="utf-8")
    print(f"[client] wrote local cache file: {local_file}")

    print("=== Step 3: put ===")
    s2 = await afs.put(path)
    print(s2)

    print("=== Step 4: re-sync ===")
    s3 = await afs.open_sync_read(path)
    print(s3)

if __name__ == "__main__":
    asyncio.run(main())
