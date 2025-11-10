"""
Worker for finding prime numbers using AFS
"""
import pickle
import sys
import random
import asyncio
from primefinder2_snapshot.prime import is_prime
from DS_DFS_AFSlike.afs.client import AFSClient
from DS_DFS_AFSlike.rpc.client import RPCClient

HOST = 'localhost'
PORT = 5000

class AFSWorker:
  def __init__(self, worker_id, afs_servers):
    self.worker_id = worker_id
    self.afs_servers = afs_servers
    self.afs: AFSClient = None
    self.path = f"primefinder2_snapshot/snapshots/worker_{worker_id}.pkl"
  async def initialize_afs(self):
    rpc = RPCClient(self.afs_servers, retries=2)
    self.afs = AFSClient(rpc)

  async def save_sp_afs(self, data: dict):
    """save worker snapshots to AFS"""
    try:
      bytes = pickle.dumps(data)
      try:
        fd = await self.afs.create(self.path)
      except:
        #file exists
        fd = await self.afs.open(self.path, mode="w")
      self.afs.write(fd, bytes)
      await self.afs.close(fd)
      print(f"Worker{self.worker_id} snapshots saved to AFS")
    except Exception as e:
      print(f"{self.worker_id}Error saving snapshot: {e}")

  async def load_sp_afs(self):
    try:
      fd = await self.afs.open(self.path, mode="r")
      content = self.afs.read(fd)
      await self.afs.close(fd)
      data = pickle.loads(content)
      print(f"Worker{self.worker_id} loads snapshot form AFS: {self.path}")
      return data
    except Exception as e:
      print(f"{self.worker_id}Cannot find snapshot: {e}")
      return None
  
  async def run(self):
    """main logic with AFS"""
    await self.initialize_afs()
    #load snapshots
    state = await self.load_sp_afs()

    if state:
      chunk_id = state.get("chunk_id")
      numbers = state.get("data", [])
      count = state.get("processed_count", 0)
      found = set(state.get("found_primes", []))
      print(f"[Worker {self.worker_id}] resuming from AFS snapshot: {count}/{len(numbers)} processed")
    else:
      chunk_id = None
      numbers = []
      count = 0
      found = set()

    reader: asyncio.StreamReader = None
    writer: asyncio.StreamWriter = None
    try:
      #connect to coordinator
      reader, writer = await asyncio.open_connection(HOST, PORT)
      print(f"[Worker {self.worker_id}] Connected to coordinator")
    except:
      print(f"[Worker {self.worker_id}] Cannot connect to coordinator, exiting")
      return False
    
    if not numbers:
      try:
        data = await reader.read(8192)
        if not data:
            print(f"[Worker {self.worker_id}] Coordinator disconnected, exiting")
            return False
        
        task = pickle.loads(data)
        if task.get("type") == "no_task":
          print(f"[Worker {self.worker_id}] No more tasks available, exiting")
          writer.close()
          await writer.wait_closed()
          return False
        chunk_id = task["chunk_id"]
        numbers = task["data"]
        count = 0
        found = set()
        await self.save_sp_afs({
          "chunk_id": chunk_id,
          "data": numbers,
          "processed_count": 0,
          "found_primes": []
        })
      except Exception as e:
        print(f"{self.worker_id}] Error receiving task: {e}")
        writer.close()
        await writer.wait_closed()
        return False
    
    try:
      for i in range(count, len(numbers)):
        n = numbers[i]
        await asyncio.sleep(0.05)
        #simulate crash
        if random.random()<0.03:
          print(f"{self.worker_id}] simulate crash at number {n} (index {i})")
          #save to afs
          await self.save_sp_afs({
            "chunk_id": chunk_id,
            "data": numbers,
            "processed_count": i,
            "found_primes": list(found)
          })
          writer.close()
          await writer.wait_closed()
          sys.exit(1)
        if is_prime(n):
          found.add(n)
        count = i+1
        #snapshot to afs every 10
        if count % 10 == 0:
          await self.save_sp_afs({
            "chunk_id": chunk_id,
            "data": numbers,
            "processed_count": count,
            "found_primes": list(found)
          })
        
      #sent to coor
      result = {"chunk_id": chunk_id, "primes": list(found)}
      writer.write(pickle.dumps(result))
      await writer.drain()
      #ackonw
      ack = await reader.read(1024)
      if ack == b"ok":
        print(f"{self.worker_id} received ackonwledg")
        #remove snapshot(delete method)
        return True
      else:
        print(f"{self.worker_id} did not receive")
        return True
    except Exception as e:
      print(f"{self.worker_id}] Error: {e}")
      return True
    finally:
      if writer:
        writer.close()
        await writer.wait_closed()

async def main(worker_id):
  afs_servers = [
    "127.0.0.1:8888",
    #Uncomment for Raft:
    #"127.0.0.1:8889",
    #"127.0.0.1:8890"
  ]
    
  worker = AFSWorker(worker_id, afs_servers)
  print(f"[Worker {worker_id}] Starting...")
  try:
    should_continue = await worker.run()
    if should_continue:
      state = await worker.load_sp_afs()
      if state:
        print(f"\n{worker_id} Task incomplete! Restart to resume:")
        print(f"python worker_afs.py {worker_id}")
      else:
        print(f"{worker_id} Task completed successfully")
    else:
      print(f"{worker_id} No more tasks available")
  
  except KeyboardInterrupt:
    print(f"\nInterrupted by user")
  except Exception as e:
    print(f"Error: {e}")
    raise


if __name__ == "__main__":
  if len(sys.argv) < 2:
    print("Usage: python worker_afs.py <worker_id>")
    sys.exit(1)
  
  worker_id = int(sys.argv[1])
  asyncio.run(main(worker_id))
