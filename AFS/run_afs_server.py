# run_afs_server.py
import asyncio
from AFS.rpc.server import RPCServer
from AFS.afs.handlers import Open, TestAuth, GetFile, PutFile, Create
import sys

async def main():
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 8888

    srv = RPCServer()
    srv.register("Open", Open)
    srv.register("TestAuth", TestAuth)
    srv.register("GetFile", GetFile)
    srv.register("PutFile", PutFile)
    srv.register("Create", Create)
    await srv.serve(host, port)

if __name__ == "__main__":
    asyncio.run(main())
