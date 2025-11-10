"""
run before start coor and worker
"""
import asyncio
import os
from AFS.afs.client import AFSClient
from AFS.rpc.client import RPCClient

async def upload_test(input_file="primefinder2_snapshot/data/test1000.txt", path="primefinder2_snapshot/data/test1000.txt"):
  print("UPLOAD TEST DATA TO AFS")
  print("=" * 60)
  #if file local
  if not os.path.exists(input_file):
    print(f"{input_file} not found")
    print(f"Need to generate test: numbers_generator.py")
    return False
  #connect
  servers = [
    "127.0.0.1:8888",
    #Uncomment for Raft:
    #"127.0.0.1:8889",
    #"127.0.0.1:8890"
  ]
  rpc = RPCClient(servers, retries=2)
  afs = AFSClient(rpc)

  #read
  with open(input_file, "r") as f:
    content = f.read()
  lines = [line.strip() for line in content.split('\n') if line.strip()]
  #send to afs
  try:
    fd = await afs.create(path)
    afs.write(fd, content.encode('utf-8'))
    await afs.close(fd)
    print(f"Upload to {path}")
  except Exception as e:
    print(f"Error: {e}")
    return False
  #verify
  try:
    fd = await afs.open(path, "r")
    uploaded = afs.read(fd)
    await afs.close(fd)
    up_lines = [line.strip() for line in uploaded.decode('utf-8').split('\n') if line.strip()]
    if len(up_lines) == len(lines):
      print("verfication pass")
    else:
      print("verifacation fail")
      return False
  except Exception as e:
    print(f"Error: {e}")
    return False
  print("UPLOAD COMPLETE")
  print("\nNext steps:")
  print("  1. Start coordinator: python coordinator_afs.py")
  print("  2. Start workers:     python worker_afs.py 1")
  print("                        python worker_afs.py 2")
  print("                        python worker_afs.py 3")
  print("                        python worker_afs.py 4")
  
  return True

async def test_connect():
  #before upload see connection
  print("Test AFS connection")
  try:
    rpc = RPCClient(["127.0.0.1:8888"], retries=1)
    afs = AFSClient(rpc)
    test_path = "/test_connection.txt"
    fd = await afs.create(test_path)
    afs.write(fd, b"test")
    await afs.close(fd)
    print("AFS connection successful")
    return True
  except Exception as e:
    print(f"Cannot connect to AFS: {e}")
    return False

async def main():
  if not await test_connect(): return
  success = await upload_test()
  if not success:
    print("Fail to upload test data")
    exit(1)

if __name__ == "__main__":
  asyncio.run(main())