"""
Simple test to connect to Raft cluster and create a file.
Demonstrates basic Raft operation with automatic leader discovery.

Usage:
    # Make sure 3 servers are running first:
    # Terminal 1: python -m raft.server server1 127.0.0.1 8888 127.0.0.1:8889 127.0.0.1:8890
    # Terminal 2: python -m raft.server server2 127.0.0.1 8889 127.0.0.1:8888 127.0.0.1:8890
    # Terminal 3: python -m raft.server server3 127.0.0.1 8890 127.0.0.1:8888 127.0.0.1:8889
    
    # Then run this test:
    python test_connect_cluster.py
"""

import asyncio
from AFS.rpc.client import RPCClient
from AFS.afs.client import AFSClient
async def main():
    print("=" * 60)
    print("TEST: Connect to Raft Cluster")
    print("\nConnecting to Raft cluster...")
    rpc = RPCClient([
        "127.0.0.1:8888",
        "127.0.0.1:8889",
        "127.0.0.1:8890"
    ])
    afs = AFSClient(rpc)
    await asyncio.sleep(5)
    try:
        fd = await afs.create("/myfile.txt")
        print(f"File created, fd={fd}")
        afs.write(fd, b"Hello Raft!\n")
        print("Data written")
        await afs.close(fd)
        print("File closed and replicated")
        fd = await afs.open("/myfile.txt", 'r')
        content = afs.read(fd)
        await afs.close(fd)
        print(f"Read: {content.decode()}")  
        print("\nSUCCESS")      
    except Exception as e:
        print(f"\nFAILED")
        print(f"Error: {e}")
        print("\nPossible causes:")
        print("  1. Servers not running")
        print("  2. No leader elected yet (wait longer)")
        print("  3. Less than majority (2/3) servers available")
if __name__ == "__main__":
    asyncio.run(main())