"""
Coordinator using AFS with Chandy-Lamport snapshot
"""
import pickle
import math
import asyncio
from typing import Set, List, Dict
from AFS.rpc.client import RPCClient
from AFS.afs.client import AFSClient
import integration.snapshot_afs as snapshot

HOST = 'localhost'
PORT = 5000
NUM_WORKERS = 4
SNAPSHOT_NAME = "coordinator_global_snapshot"
#afs path
INPUT_FILE = "/primefinder/data/test1000.txt"
OUTPUT_FILE = "/primefinder/outputs/primes_distributed.txt"

class AFSCoordinator:
  def __init__(self, afs_servers):
    self.afs_servers = afs_servers
    self.afs: AFSClient = None
    self.primes: Set[int] = set()
    self.workers: Dict[int, asyncio.StreamWriter] = {}
    self.finished_workers: Set[int] = set()
    self.chunks: List[List[int]] = []
    self.next_worker_id = 1

  async def initialize_afs(self):
    rpc = RPCClient(self.afs_servers, retries=2)
    self.afs = AFSClient(rpc=rpc)
    snapshot.init(NUM_WORKERS)

  async def read_from_afs(self, path: str):
    try:
      fd = await self.afs.open(path, mode="r")
      content = self.afs.read(fd)
      await self.afs.close(fd)
      lines = content.decode('utf-8').split("\n")
      numbers = [int(line.strip()) for line in lines if line.strip()]
      print(f"Coordinator read {len(numbers)} numbers from AFS")
      return numbers
    except Exception as e:
      print(f"Error reading from AFS: {e}")
      raise

  async def save_sp_afs(self, path: str, primes: set):
    """save coordinator snapshots to AFS"""
    try:
      result = "\n".join(str(p) for p in sorted(primes))+"\n"
      try:
        fd = await self.afs.create(path)
      except:
        fd = await self.afs.open(path, mode="w")
      self.afs.write(fd, result.encode('utf-8'))
      await self.afs.close(fd)
      print(f"Coordinator save results: {len(primes)}")
    except Exception as e:
      print(f"Error saving primes: {e}")

  def split_numbers(self, numbers, workers):
    chunk_size = math.ceil(len(numbers)/workers)
    chunks = []
    i = 0
    while i < len(numbers):
        chunk = numbers[i:i + chunk_size]
        chunks.append(chunk)
        i += chunk_size
    return chunks
  
  async def snapshot_loop(self):
    #gloabl snapshot perodic
    while True:
      await asyncio.sleep(1)
      sid = snapshot.start(len(self.primes))
      if sid:
        print(f"Coordinator starting snapshot {sid}")
        #send marker
        marker = pickle.dumps({"type": "marker", "snapshot_id": sid})
        for worker_id, writer in self.workers.items():
          try:
            writer.write(marker)
            await writer.drain()
          except ConnectionError:
            print(f"Error sending marker to worker {worker_id} (disconnected)")
  
  async def handle_worker(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    ad = writer.get_extra_info('peername')
    print(f"Worker connected from {ad}")
    if self.next_worker_id > NUM_WORKERS:
      print("Max workers reached, rejecting connection")
      writer.close()
      await writer.wait_closed()
      return
    
    worker_id = self.next_worker_id
    self.next_worker_id += 1
    self.workers[worker_id] = writer
    
    if worker_id <= len(self.chunks):
      task_chunk = self.chunks[worker_id - 1]
      task = pickle.dumps({"type":"task", "chunk":task_chunk, "workerid": worker_id})
      try:
          writer.write(task)
          await writer.drain()
      except Exception as e:
          print(f"Error sending init task to {worker_id}: {e}")
          return
    try:
      while True:
        data = await reader.read(4096)
        if not data:
          raise ConnectionError("Worker disconnected")
        message = pickle.loads(data)
        type = message.get("type")

        if type == "result":
          self.primes.add(message["prime"])
          snapshot.save_inflight(worker_id, message["prime"])
          
        elif type == "marker":
          await snapshot.save_snapshot(
              self.afs,
              SNAPSHOT_NAME,
              message["workerid"],
              message["snapshot_id"],
              message["state"]
          )
        
        elif type == "reconnect":
          wid = message["workerid"]
          self.workers[wid] = writer
          print(f"[Coordinator] Worker {wid} RECONNECTED.")
          
          if wid <= len(self.chunks):
            original_chunk = self.chunks[wid - 1]
            task = {"type":"task", "chunk": original_chunk, "workerid": wid}
            writer.write(pickle.dumps(task))
            await writer.drain()
            print(f"[Coordinator] Resent task to Worker {wid}")

        elif type == "finish":
          print(f"Worker {worker_id} finished")
          self.finished_workers.add(worker_id)
          break

    except (ConnectionError, EOFError, ConnectionResetError) as e:
      print(f"Worker {worker_id} (from {ad}) disconnected: {e}")
    except Exception as e:
      print(f"Error with worker {worker_id}: {e}")
    finally:
      if worker_id in self.workers:
        pass
      if not writer.is_closing():
        writer.close()
        await writer.wait_closed()

  async def run(self):
    await self.initialize_afs()
    numbers = await self.read_from_afs(INPUT_FILE)
    self.tasks = self.split_numbers(numbers, NUM_WORKERS)
    self.chunks = self.split_numbers(numbers, NUM_WORKERS)
    snapshot_task = asyncio.create_task(self.snapshot_loop())

    server = await asyncio.start_server(self.handle_worker, HOST, PORT)
    addr = server.sockets[0].getsockname()
    print(f"Coordinator listening on {addr[0]}:{addr[1]}, waiting for {NUM_WORKERS} workers")

    while len(self.finished_workers) < NUM_WORKERS:
      await asyncio.sleep(1)

    print("All workers finished.")

    server.close()
    await server.wait_closed()
    snapshot_task.cancel()
    print(f"Total primes found: {len(self.primes)}")
    await self.save_to_afs(OUTPUT_FILE, self.primes)

async def main():
  afs_servers = [
    "127.0.0.1:8888",
    #"127.0.0.1:8889",
    #"127.0.0.1:8890"
  ]
  coordinator = AFSCoordinator(afs_servers)
  try:
    await coordinator.run()
  except KeyboardInterrupt:
    print("\nKeyboard interrupted.")
  except Exception as e:
    print(f"Coordinator error: {e}")
    raise

if __name__ == "__main__":
  asyncio.run(main())

