"""
Test script for Raft-replicated AFS cluster
Tests:
- Leader election
- Log replication
- Client operations
- Failure recovery
"""
import asyncio
import time
from afs.client import AFSClient
from rpc.client import RPCClient

async def test_basic():
  print("Test 1: Basic File Operations")
  print("=" * 60)
  #3 servers
  rpc = RPCClient(
    [
      "127.0.0.1:8888",
      "127.0.0.1:8889",
      "127.0.0.1:8890"
    ], retries=2
  )
  afs = AFSClient(rpc)
  #wait for leader election
  await asyncio.sleep(5)
  #create file
  print("\nCreating file /test_raft.txt")
  try:
    file = await afs.create("/test_raft.txt")
    print(f"Created file, fd={file}")
    #write
    data = b"Test data for cluster\n"
    afs.write(fd=file, data=data)
    print(f"{len(data)} bytes wrote")
    #close
    await afs.close(fd=file)
    print(f"File closed and replicated")
  except Exception as e:
    print(f"Test Error: {e}")
    return
  
  #read file
  print("\nReading file")
  try:
    file = await afs.open("/test_raft.txt", mode="r")
    content = afs.read(fd=file)
    await afs.close(fd=file)
    print(f"Read back: {content.decode('utf-8')}")
  except Exception as e:
    print(f"Test Error:{e}")

async def test_read_from_follower():
  print("\n" + "=" * 60)
  print("Test 2: Reads served by followers")
  #force connect to 1 server
  rpc = RPCClient(["127.0.0.1:8889"], retries=0)
  try:
    r = await rpc.call("GetFile", {"path": "/test_raft.txt"})
    if r["code"] == 0:
      print(f"Successfully read from follower")
    else:
      print(f"Response: {r}")
  except Exception as e:
    print("Test Error: {e}")

async def test_write_to_follower():
  print("\n" + "=" * 60)
  print("Test 3: Writes are redirected on followers")
  #force connect to 1 server
  rpc = RPCClient(["127.0.0.1:8889"], retries=0)
  try:
    r = await rpc.call("Create", {"path": "/follower_test.txt"})
    if r.get("data", {}).get("error") == "not_leader":
      print("Correctly rejected write on follower")
      last_leader = r.get("data", {}).get("leader_hint")
      if last_leader:
        print(f"Last known leader: {last_leader}")
    elif r["code"] == 0:
      print(f"Server was the leader")
    else:
      print(f"Response: {r}")
  except Exception as e:
    print(f"Test Error: {e}")
      
async def test_leader():
  """Test failover when leader crash"""
  print("\n" + "=" * 60)
  print("Test 4: Leader Failure and Recovery")
  print("Manual test: kill the leader process and see if a new leader is elected within election timeout")
  print("Then try file operations again.")

async def test_stauts():
  print("\n" + "=" * 60)
  print("Cluster Status Check")
  servers = [
    ("server1", "127.0.0.1:8888"),
    ("server2", "127.0.0.1:8889"),
    ("server3", "127.0.0.1:8890")
  ]
  for name, ad in servers:
    rpc = RPCClient([ad], read_timeout=1.0, retries=0)
    try:
      r = await rpc.call("Open", {"path": "/test_raft.txt"})
      if r["code"] == 0:
        status = "ALIVE"
      else:
        sttus = f"ALIVE (err: {r.get('err', 'unknown')})"
    except Exception as e:
      status = f"DOWN ({str(e)[:30]})"
    print(f"{name:10s} ({ad:20s}) : {status}")

async def main():
  print("\n" + "=" * 60)
  print("RAFT TEST")
  print("\nMake sure you have started 3 servers")
  print("\nPress Enter to continue")
  input()

  await test_stauts()
  await test_basic()
  await test_read_from_follower()
  await test_write_to_follower()
  await test_leader()

  print("TEST COMPLETE")
  print("\n" + "=" * 60)

if __name__ == "__main__":
  asyncio.run(main())