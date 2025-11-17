import asyncio
import os
from AFS.afs.client import AFSClient
from AFS.rpc.client import RPCClient

FILES_TO_UPLOAD = {
    "tests/data/input_0.txt": "/tests/data/input_0.txt",
    "tests/data/input_1.txt": "/tests/data/input_1.txt",
    "tests/data/input_2.txt": "/tests/data/input_2.txt"
}

async def upload():
    print("Connecting to AFS...")
    rpc = RPCClient(["127.0.0.1:8888"], retries=2)
    afs = AFSClient(rpc)

    for local_path, afs_path in FILES_TO_UPLOAD.items():
        if not os.path.exists(local_path):
            print(f"Skipping {local_path}: Local file not found.")
            continue
            
        print(f"Uploading local '{local_path}' to AFS '{afs_path}'...")
        
        with open(local_path, "r") as f:
            content = f.read()
            
        try:
            fd = await afs.create(afs_path)
        except Exception:
            fd = await afs.open(afs_path, "w")
            
        await afs.write(fd, content.encode('utf-8'))
        await afs.close(fd)
        print(f"Success: {afs_path} uploaded.")

if __name__ == "__main__":
    asyncio.run(upload())