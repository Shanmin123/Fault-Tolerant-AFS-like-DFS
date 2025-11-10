"""
Worker for finding prime numbers(stateless)
"""
import pickle
import sys
import asyncio
import random
from primefinder.prime import is_prime
#reconnect
from AFS.rpc.client import RPCClient
from AFS.afs.client import AFSClient

HOST = 'localhost'
PORT = 5000
SNAPSHOT_LATEST = "snapshots/snapshot_latest.pkl"

class AFSWorker:
  def __init__(self, worker_id, afs_servers):
    self.worker_id = worker_id
    self.afs_servers = afs_servers
    self.afs: AFSClient = None
  
  async def initialize_afs(self):
    rpc = RPCClient(self.afs_servers, retries=2)
    self.afs = AFSClient(rpc)

  async def load_snapshot(self):
    #read latest snapshot from afs
    try:
      try:
        fd = await self.afs.open(SNAPSHOT_LATEST, mode="r")
      except Exception:
          return 0
      content = self.afs.read(fd)
      await self.afs.close(fd)
      
      if not content:
        return 0
      snap = pickle.loads(content)
      worker_state = snap.get("worker_state", {})
      if self.worker_id in worker_state:
        numid = worker_state[self.worker_id]["numid"]
        print(f"[Worker {self.worker_id}] recover from index {numid}")
        return numid
      return 0
    except Exception as e:
      print(f"[Worker {self.worker_id}] Snapshot check failed (continuing as new): {e}")
      return 0

async def run(self):
  await self.initialize_afs()
  #reconnect
  start_numid = await self.load_snapshot()
  #connect to coor
  reader: asyncio.StreamReader = None
  writer: asyncio.StreamWriter = None
  try:
    reader, writer = await asyncio.open_connection(HOST, PORT)
    print(f"[Worker {self.worker_id}] Connected to coordinator")
  except Exception as e:
      print(f"Connection failed: {e}")
      return
  
  #re-> reconnect
  if start_numid > 0:
    msg = {
      "type": "reconnect", 
      "workerid": self.worker_id, 
      "numid": start_numid
    }
    writer.write(pickle.dumps(msg))
    await writer.drain()
    print(f"[Worker {self.worker_id}] Sent reconnect message")
  #task
  data = await reader.read(8192)
  if not data:
    return
  task = pickle.loads(data)
  numbers = task["chunk"]
  if task.get("workerid"):
    self.worker_id = task["workerid"]
  print(f"[Worker {self.worker_id}] Processing chunk size: {len(numbers)}")

  numid = 0 
  found = []
  seen_marker = {}
  
  while numid < len(numbers):
    if numid < start_numid:
      numid += 1
      continue

    #simulate crash
    #0.05% crash
    if random.random() < 0.0005:
      print(f"[Worker {self.worker_id}]SIMULATING CRASH at index {numid}")
      return

    try:
      marker = await asyncio.wait_for(reader.read(4096), timeout=0.001)
      if marker:
        message = pickle.loads(marker)
        if message.get("type") == "marker":
            sid = message["snapshot_id"]
            if not seen_marker.get(sid, False):
                seen_marker[sid] = True
                state = {
                  "workerid": self.worker_id,
                  "numid": numid,
                  "found_count": len(found)
                }
                marker_resp = pickle.dumps({
                  "type": "marker",
                  "snapshot_id": sid,
                  "state": state
                })
                writer.write(marker_resp)
                await writer.drain()
    except asyncio.TimeoutError:
      pass 
    except Exception as e:
      print(f"Error reading: {e}")
      break

    n = numbers[numid]
    if is_prime(n):
      found.append(n)
      result = pickle.dumps({"type": "result", "prime": n, "workerid": self.worker_id})
      writer.write(result)
      await writer.drain()
        
    numid += 1
    await asyncio.sleep(0.002)

  finish = pickle.dumps({"type": "finish", "workerid": self.worker_id})
  writer.write(finish)
  await writer.drain()
  print(f"[Worker {self.worker_id}] Finished")
  
  writer.close()
  await writer.wait_closed()

async def main(worker_id):
  afs_servers = [
    "127.0.0.1:8888", 
    #"127.0.0.1:8889", 
    #"127.0.0.1:8890"
    ]
  worker = AFSWorker(worker_id, afs_servers)
  await worker.run()

if __name__ == "__main__":
  if len(sys.argv) < 2:
    sys.exit(1)
  worker_id = sys.argv[1]
  asyncio.run(main(worker_id))
