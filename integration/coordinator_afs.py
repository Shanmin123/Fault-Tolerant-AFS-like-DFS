"""
Coordinator for distributed prime number finding using AFS
"""
import pickle
import math
import asyncio
from typing import Set, List, Dict
from DS_DFS_AFSlike.rpc.client import RPCClient
from DS_DFS_AFSlike.afs.client import AFSClient

HOST = 'localhost'
PORT = 5000
NUM_WORKERS = 4
SNAPSHOT = "coordinator_snapshot"

class AFSCoordinator:
  #coordinator now uses AFS
  def __init__(self, afs_servers):
    self.afs_servers = afs_servers
    self.afs: AFSClient = None

    self.rest_queue: asyncio.Queue = None
    self.primes: Set[int] = set()
    self.done_chunks: List[int] = []
    self.active_chunks: Dict[int, List[int]] = {}
  async def initialize_afs(self):
    rpc = RPCClient(self.afs_servers, retries=2)
    self.afs = AFSClient(rpc=rpc)
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
  async def save_sp_afs(self, snapshot: str, data: dict):
    """save coordinator snapshots to AFS"""
    path = f"primefinder2_snapshot/snapshots/{snapshot}.pkl"
    try:
      bytes = pickle.dumps(data)
      try:
        fd = await self.afs.create(path)
      except:
        #file exists
        fd = await self.afs.open(path, mode="w")
      self.afs.write(fd, bytes)
      await self.afs.close(fd)
      print(f"Coordinator snapshots saved to AFS: {path}")
    except Exception as e:
      print(f"Error saving snapshot: {e}")
  async def load_sp_afs(self, snapshot: str):
    """Load snapshot from AFS"""
    path = f"primefinder2_snapshot/snapshots/{snapshot}.pkl"
    try:
      fd = await self.afs.open(path, mode="r")
      content = self.afs.read(fd)
      await self.afs.close(fd)
      data = pickle.loads(content)
      print(f"Coordinator loads snapshot form AFS: {path}")
      return data
    except Exception as e:
      print(f"Cannot find snapshot: {e}")
      return None
  async def save_to_afs(self, path: str, primes: set):
    """save final primes results to AFS"""
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
  
  @staticmethod
  def split_numbers(numbers, workers):
    size = math.ceil(len(numbers)/workers)
    return [numbers[i:i+size] for i in range(0, len(numbers),size)]
  
  async def _get_snapshot_data(self):
    """Helper to get current state for snapshotting"""
    remaining_chunks = []
    while not self.rest_queue.empty():
      remaining_chunks.append(self.rest_queue.get_nowait())
    all_rest = remaining_chunks + list(self.active_chunks.values())
    for chunk in all_rest:
      await self.rest_queue.put(chunk)
    return {
      "rest": all_rest,
      "done": self.done_chunks,
      "primes": list(self.primes)
    }
  async def handle_worker(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    ad = writer.get_extra_info('peername')
    chunk = None
    chunk_id = None
    try:
      chunk = await self.rest_queue.get()
      chunk_id = hash(tuple(chunk))
      self.active_chunks[chunk_id] = chunk
      task = {
        "type": "task",
        "chunk_id": chunk_id,
        "data": chunk
      }
      print(f"Assigning chunk {chunk_id} to {ad}")
      await self.save_sp_afs(SNAPSHOT, await self._get_snapshot_data())
      writer.write(pickle.dumps(task))
      await writer.drain()
      data = await asyncio.wait_for(reader.read(8192), timeout=60.0)
      if not data:
        raise ConnectionError("Worker disconnected before sending result")
      result = pickle.loads(data)

      self.primes.update(result["primes"])
      self.done_chunks.append(result["chunk_id"])
      del self.active_chunks[chunk_id]
      print(f"Receive result for chunk {chunk_id}: {len(result['primes'])} primes")

      writer.write(b"ok")
      await writer.drain()

      await self.save_sp_afs(SNAPSHOT, await self._get_snapshot_data())
      self.rest_queue.task_done()
    except (asyncio.TimeoutError, ConnectionError, EOFError, ConnectionResetError) as e:
      print(f"Worker {ad} failed on chunk {chunk_id}: {e}")
      if chunk:
        await self.rest_queue.put(chunk)
        print(f"Re-queued chunk {chunk_id}")
      if chunk_id in self.active_chunks:
        del self.active_chunks[chunk_id]
    except Exception as e:
      print(f"Unhandled error for worker {ad}: {e}")
      if chunk:
        await self.rest_queue.put(chunk)
      if chunk_id in self.active_chunks:
        del self.active_chunks[chunk_id]
    finally:
      print(f"Worker {ad} disconnected")
      writer.close()
      await writer.wait_closed()
  async def shutdown_when_done(self, server: asyncio.Server):
    await self.rest_queue.join()
    print("All tasks from queue have been processed.")
    server.close()

  async def run(self, input = "/primefinder2_snapshot/data/test1000.txt", output = "/outputs/primes_distributed.txt"):
    """main logic with AFS"""
    await self.initialize_afs()
    self.rest_queue = asyncio.Queue()
    self.primes = set()
    self.done_chunks = []
    self.active_chunks = {}
    snapshot = await self.load_sp_afs(snapshot=SNAPSHOT)
    if snapshot is None:
      #read from AFS
      numbers = await self.read_from_afs(input)
      rest = self.split_numbers(numbers, NUM_WORKERS)
      print(f"Starting with {len(rest)} chunks")
    else:
      #restore
      rest = snapshot.get('rest', [])
      self.done_chunks = snapshot.get('done', [])
      self.primes = set(snapshot.get('primes', []))
      print(f"Restored: {len(rest)} chunks remain, {len(self.done_chunks)} chunks done, {len(self.primes)} primes found")
    
    for chunk in rest:
      await self.rest_queue.put(chunk)
    #start server
    server = await asyncio.start_server(self.handle_worker, HOST, PORT)
    ad = server.sockets[0].getsockname()
    print(f"Coordinator listening on {ad[0]}:{ad[1]}")

    shutdown_task = asyncio.create_task(self.shutdown_when_done(server))

    try:
      await server.serve_forever()
    except asyncio.CancelledError:
      print("Server is shutting down.")
    await shutdown_task
    print(f"All tasks completed")
    print(f"Total primes: {len(self.primes)}")

    await self.save_to_afs(output, self.primes)


async def main():
  afs_servers = [
    "127.0.0.1:8888",
    # Uncomment for Raft:
    #"127.0.0.1:8889",
    #"127.0.0.1:8890"
  ]
  coordinator = AFSCoordinator(afs_servers)
  try:
    await coordinator.run(
      input="/primefinder2_snapshot/data/test1000.txt",
      output="/outputs/primes_distributed.txt"
    )
  except KeyboardInterrupt:
    print("Keyboard interrupt")
  except Exception as e:
    print(f"Error: {e}")
    raise

if __name__ == "__main__":
  asyncio.run(main())