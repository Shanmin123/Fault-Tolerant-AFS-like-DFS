# afs_smoke.py
import asyncio, base64
from rpc.client import RPCClient

async def main():
    c = RPCClient(["127.0.0.1:8888"])
    path = "/demo.txt"

    r1 = await c.call("Open", {"path": path})
    print("[client] Open:", r1)

    cv = 0
    r2 = await c.call("TestAuth", {"path": path, "client_version": cv})
    print("[client] TestAuth:", r2)

    if r2["data"]["changed"]:
        try:
            r3 = await c.call("GetFile", {"path": path})
            print("[client] GetFile: version", r3["data"]["version"])
        except Exception as e:
            print("[client] GetFile error:", e)

    payload = base64.b64encode(b"Hello AFS\n").decode("ascii")
    r4 = await c.call("PutFile", {"path": path, "bytes": payload, "base_version": r1["data"]["version"]})
    print("[client] PutFile:", r4)

    r5 = await c.call("GetFile", {"path": path})
    print("[client] GetFile after put: version", r5["data"]["version"])

if __name__ == "__main__":
    asyncio.run(main())
