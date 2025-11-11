"""
run before start coor and worker
"""
"""
Upload test data to AFS
"""
import asyncio
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from AFS.afs.client import AFSClient
from AFS.rpc.client import RPCClient

LOCAL_INPUT_FILE = "primefinder/data/test10000.txt"
AFS_INPUT_PATH = "/primefinder/data/test10000.txt"


async def upload_data():
    print("Uploading test data to AFS...")
    print("=" * 60)

    # Check if local file exists
    if not os.path.exists(LOCAL_INPUT_FILE):
        print(f"Error: {LOCAL_INPUT_FILE} not found")
        print("Please run: python -m primefinder.numers_generator")
        return False

    # Connect to AFS
    servers = ["127.0.0.1:8888"]
    rpc = RPCClient(servers, retries=2)
    afs = AFSClient(rpc)

    # Read local file
    with open(LOCAL_INPUT_FILE, "r") as f:
        content = f.read()

    print(f"Read {len(content.split())} lines from {LOCAL_INPUT_FILE}")

    # Upload to AFS
    try:
        fd = await afs.create(AFS_INPUT_PATH)
        afs.write(fd, content.encode('utf-8'))
        await afs.close(fd)
        print(f"✓ Uploaded to AFS: {AFS_INPUT_PATH}")
    except Exception as e:
        print(f"✗ Upload error: {e}")
        return False

    # Verify upload
    try:
        fd = await afs.open(AFS_INPUT_PATH, "r")
        uploaded = afs.read(fd)
        await afs.close(fd)

        if uploaded.decode('utf-8') == content:
            print("✓ Verification passed")
        else:
            print("✗ Verification failed")
            return False
    except Exception as e:
        print(f"✗ Verification error: {e}")
        return False

    print("\n" + "=" * 60)
    print("Upload complete! You can now run:")
    print("  python -m primefinder.coordinator")
    return True


if __name__ == "__main__":
    asyncio.run(upload_data())