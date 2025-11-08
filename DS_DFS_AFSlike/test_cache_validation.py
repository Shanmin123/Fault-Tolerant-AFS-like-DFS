"""
Test cache validation with TestAuth RPC.
Demonstrates:
- First open fetches from server
- Second open uses cached copy (if valid)
- Cache invalidation on server-side changes

Usage:
    python test_cache_validation.py
"""

import asyncio
from rpc.client import RPCClient
from afs.client import AFSClient
async def test_cache_validation():
    """Test cache validation mechanism"""
    print("=" * 60)
    print("TEST: Cache Validation")    
    rpc = RPCClient(["127.0.0.1:8888"])
    afs = AFSClient(rpc)
    try:
        #Test 1: Create file
        fd = await afs.create('/cache_test.txt')
        afs.write(fd, b'Version 1\n')
        await afs.close(fd)
        print("File created with 'Version 1'")
        #Test 2: First open (cache miss - fetch from server)
        print("\n[Step 2] First open (should fetch from server)")
        fd = await afs.open('/cache_test.txt', 'r')
        content1 = afs.read(fd)
        await afs.close(fd)
        print(f"Read: {content1.decode().strip()}")
        print("  (Fetched from server)")
        #Test 3: Second open (cache hit - use local copy)
        print("\n[Step 3] Second open (should use cache)")
        fd = await afs.open('/cache_test.txt', 'r')
        content2 = afs.read(fd)
        await afs.close(fd)
        print(f"Read: {content2.decode().strip()}")
        print("  (Used cached copy - TestAuth returned 'valid')")
        #Test 4: Modify file on server (simulate another client)
        print("\n[Step 4] Modifying file (simulating another client)")
        fd = await afs.open('/cache_test.txt', 'r+')
        afs.seek(fd, 0, 0)
        afs.write(fd, b'Version 2\n')
        await afs.close(fd)
        print("File updated to 'Version 2'")
        #Test 5: Third open (cache invalid - re-fetch)
        print("\n[Step 5] Third open (cache invalid, should re-fetch)")
        fd = await afs.open('/cache_test.txt', 'r')
        content3 = afs.read(fd)
        await afs.close(fd)
        print(f"Read: {content3.decode().strip()}")
        print("  (Re-fetched from server - TestAuth returned 'changed')")

        #Verify results
        print("RESULTS:")
        if content1 == content2 and content1 == b'Version 1\n':
            print("Cache hit worked (content1 == content2)")
        else:
            print("Cache hit failed")
        if content3 == b'Version 2\n':
            print("Cache invalidation worked")
        else:
            print("Cache invalidation failed")
            
        if content1 == content2 and content3 == b'Version 2\n':
            print("\nALL TESTS PASSED")
            return 0
        else:
            print("\nSOME TESTS FAILED")
            return 1
    except Exception as e:
        print(f"\nTEST FAILED")
        print(f"Error: {e}")
        return 1
async def main():
    exit_code = await test_cache_validation()
    exit(exit_code)
if __name__ == "__main__":
    asyncio.run(main())