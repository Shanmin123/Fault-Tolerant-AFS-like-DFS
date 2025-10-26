# run_afs_server.py
import asyncio
from rpc.server import RPCServer
from afs.handlers import Open, TestAuth, GetFile, PutFile

async def main():
    srv = RPCServer()
    srv.register("Open", Open)
    srv.register("TestAuth", TestAuth)
    srv.register("GetFile", GetFile)
    srv.register("PutFile", PutFile)
    await srv.serve("127.0.0.1", 8888)

if __name__ == "__main__":
    asyncio.run(main())
