import random
import asyncio
import sys
from pathlib import Path
from AFS.afs.client import AFSClient
from AFS.rpc.client import RPCClient


LOCAL_FILE_PATH = Path("tests/data/test10000.txt")
AFS_FILE_PATH = "/tests/data/input_large.txt"
AFS_SERVERS = ["127.0.0.1:8888"]

def create_local_file_if_not_exists():
  if LOCAL_FILE_PATH.exists():
    return
  try:
    LOCAL_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(LOCAL_FILE_PATH, 'w') as f:
      for _ in range(10000):
          f.write(str(random.randint(2, 10**7)) + '\n')
    
  except Exception as e:
    sys.exit(1)

async def upload_file_to_afs():
  try:
    rpc = RPCClient(AFS_SERVERS, retries=2)
    afs = AFSClient(rpc)
    
    with open(LOCAL_FILE_PATH, "r") as f:
      content_bytes = f.read().encode('utf-8')
    
    fd = -1
    try:
      fd = await afs.create(AFS_FILE_PATH)
      print(f"AFS: Created new file at {AFS_FILE_PATH}")
    except Exception as e:
      print(f"AFS: File exists, opening in write-mode to overwrite...")
      try:
        fd = await afs.open(AFS_FILE_PATH, "w")
      except Exception as open_e:
        print(f"AFS: Fatal - Failed to open existing file: {open_e}")
        sys.exit(1)

    await afs.write(fd, content_bytes)
    await afs.close(fd)
    print(f"AFS: Successfully uploaded and closed file {AFS_FILE_PATH}")

  except Exception as e:
    print(f"AFS: An unexpected error occurred: {e}")
    sys.exit(1)
async def main():
  create_local_file_if_not_exists()
  await upload_file_to_afs()

if __name__ == "__main__":
  if not Path("AFS").is_dir():
    sys.exit(1)
  asyncio.run(main())