import asyncio
import os
from AFS.afs.client import AFSClient
from AFS.rpc.client import RPCClient

# 1. 修改这里：定义你要上传的3个文件
# 格式： "本地路径": "AFS路径"
FILES_TO_UPLOAD = {
    "tests/data/input_001.txt": "/tests/data/input_001.txt",
    "tests/data/input_002.txt": "/tests/data/input_002.txt",
    "tests/data/input_003.txt": "/tests/data/input_003.txt"
}

async def upload():
    print("Connecting to AFS Cluster...")
    
    # 2. 修改这里：填入所有3个Server的地址，以便找到Leader
    servers = ["127.0.0.1:8888", "127.0.0.1:8889", "127.0.0.1:8890"]
    
    rpc = RPCClient(servers, retries=2)
    afs = AFSClient(rpc)

    for local_path, afs_path in FILES_TO_UPLOAD.items():
        if not os.path.exists(local_path):
            print(f"Skipping {local_path}: Local file not found. (Did you run 'echo' commands?)")
            continue
            
        print(f"Uploading local '{local_path}' to AFS '{afs_path}'...")
        
        with open(local_path, "r") as f:
            content = f.read()
            
        try:
            # 尝试创建新文件
            fd = await afs.create(afs_path)
        except Exception:
            # 如果文件已存在，以写入模式打开（覆盖）
            fd = await afs.open(afs_path, "w")
            
        await afs.write(fd, content.encode('utf-8'))
        await afs.close(fd) # Close 触发 Raft 复制
        print(f"Success: {afs_path} uploaded.")

if __name__ == "__main__":
    asyncio.run(upload())