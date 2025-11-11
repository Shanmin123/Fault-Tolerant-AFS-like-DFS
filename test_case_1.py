import asyncio
from AFS.afs.client import AFSClient
from AFS.rpc.client import RPCClient

LOCAL_FILE = "tests/data/input_dataset_001.txt"
AFS_PATH = "/tests/data/input_dataset_001.txt"

async def upload_file():
    print(f"Connecting to AFS...")
    servers = ["127.0.0.1:8888"]
    rpc = RPCClient(servers, retries=2)
    afs = AFSClient(rpc)

    try:
        with open(LOCAL_FILE, "r") as f:
            content = f.read()
        
        print(f"Read {len(content.splitlines())} lines from {LOCAL_FILE}")

        try:
            fd = await afs.create(AFS_PATH)
        except Exception as e:
            print(f"File might exist, opening: {e}")
            fd = await afs.open(AFS_PATH, "w")
            
        await afs.write(fd, content.encode('utf-8'))
        await afs.close(fd)
        
        print(f"Successfully uploaded test file to AFS at {AFS_PATH}")

    except Exception as e:
        print(f"Error during upload: {e}")
        raise

if __name__ == "__main__":
    try:
        asyncio.run(upload_file())
    except Exception as e:
        print(f"Failed to setup test case: {e}")