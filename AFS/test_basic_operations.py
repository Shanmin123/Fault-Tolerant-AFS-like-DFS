"""
Test basic file operations (create, write, read, close).
Works with single server or Raft cluster.

Usage:
    python test_basic_operations.py                    # Single server
    python test_basic_operations.py --raft             # Raft cluster
"""

import asyncio
import sys
from AFS.rpc.client import RPCClient
from AFS.afs.client import AFSClient

async def test_basic_operations(use_raft=False):
    """Test basic file operations"""
    print("=" * 60)
    print("TEST: Basic File Operations")    
    # Connect to server(s)
    if use_raft:
        servers = [
            "127.0.0.1:8888",
            "127.0.0.1:8889",
            "127.0.0.1:8890"
        ]
        print(f"Connecting to Raft cluster: {servers}")
    else:
        servers = ["127.0.0.1:8888"]
        print(f"Connecting to single server: {servers}")
    rpc = RPCClient(servers, retries=2)
    afs = AFSClient(rpc)
    try:
        #Test 1: Create file
        fd = await afs.create('/test.txt')
        print(f"Created file, fd={fd}")
        #Test 2: Write data
        data = b'Test data\n'
        nbytes = afs.write(fd, data)
        print(f"Wrote {nbytes} bytes")
        #Test 3: Close (flush to server)
        await afs.close(fd)
        print("File closed and flushed")
        #Test 4: Read back
        fd = await afs.open('/test.txt', 'r')
        content = afs.read(fd)
        await afs.close(fd)
        print(f"Read: {content.decode()}")
        # Verify content
        if content == data:
            print("\nALL TESTS PASSED")
            return 0
        else:
            print(f"\nCONTENT MISMATCH")
            print(f"Expected: {data}")
            print(f"Got:      {content}")
            return 1
    except Exception as e:
        print(f"\nTEST FAILED")
        print(f"Error: {e}")
        return 1
async def main():
    use_raft = "--raft" in sys.argv
    exit_code = await test_basic_operations(use_raft)
    sys.exit(exit_code)
if __name__ == "__main__":
    asyncio.run(main())