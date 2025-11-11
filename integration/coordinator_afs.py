"""
Coordinator using AFS with Chandy-Lamport snapshot (FIXED)
"""
import pickle
import math
import asyncio
import sys
from typing import Set, List, Dict
from AFS.rpc.client import RPCClient
from AFS.afs.client import AFSClient
import integration.snapshot_afs as snapshot

async def send_msg(writer: asyncio.StreamWriter, obj):
    data = pickle.dumps(obj)
    writer.write(len(data).to_bytes(4, "big"))
    writer.write(data)
    await writer.drain()

async def recv_msg(reader: asyncio.StreamReader):
    header = await reader.readexactly(4)
    n = int.from_bytes(header, "big")
    payload = await reader.readexactly(n)
    return pickle.loads(payload)

HOST = 'localhost'
PORT = 5000
NUM_WORKERS = 4
SNAPSHOT_NAME = "coordinator_global_snapshot"

class AFSCoordinator:
  def __init__(self, afs_servers):
    self.afs_servers = afs_servers
    self.afs: AFSClient = None
    self.primes: Set[int] = set()
    self.workers: Dict[int, asyncio.StreamWriter] = {}
    
    self.tasks: List[List[int]] = []
    self.tasks_in_progress: Dict[int, List[int]] = {}
    self.total_tasks = 0
    self.finished_tasks_count = 0
    
    self.next_worker_id = 1

  async def initialize_afs(self):
    rpc = RPCClient(self.afs_servers, retries=2)
    self.afs = AFSClient(rpc=rpc)
    snapshot.init(NUM_WORKERS)

  async def read_from_afs(self, path: str):
    try:
      fd = await self.afs.open(path, mode="r")
      content = await self.afs.read(fd)
      await self.afs.close(fd)
      
      lines = content.decode('utf-8').split("\n")
      numbers = [int(line.strip()) for line in lines if line.strip()]
      print(f"Coordinator read {len(numbers)} numbers from AFS")
      return numbers
    except Exception as e:
      print(f"Error reading from AFS: {e}")
      raise

  async def save_results_to_afs(self, path: str, primes: set):
    """save final results to AFS"""
    try:
      result = "\n".join(str(p) for p in sorted(primes))+"\n"
      try:
        fd = await self.afs.create(path)
      except:
        fd = await self.afs.open(path, mode="w")
        
      await self.afs.write(fd, result.encode('utf-8'))
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
    while True:
       #snapshot frequency
      await asyncio.sleep(10)
      sid = snapshot.start(len(self.primes))
      if sid:
        print(f"Coordinator starting snapshot {sid}")
        #send marker
        marker_msg = {"type": "marker", "snapshot_id": sid}
        for worker_id, writer in list(self.workers.items()):
          try:
            await send_msg(writer, marker_msg)
          except ConnectionError:
            print(f"Error sending marker to worker {worker_id} (disconnected)")
  
  async def handle_worker(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    ad = writer.get_extra_info('peername')
    worker_id = self.next_worker_id
    self.next_worker_id += 1
    self.workers[worker_id] = writer
    print(f"Worker {worker_id} connected from {ad}")
    
    task_chunk = None
    try:
      if not self.tasks:
        print(f"No tasks left for worker {worker_id}")
        return 
        
      task_chunk = self.tasks.pop(0)
      self.tasks_in_progress[worker_id] = task_chunk
      task_msg = {"type":"task", "chunk":task_chunk, "workerid": worker_id}
      
      await send_msg(writer, task_msg)
      
      while True:
        try:
          message = await recv_msg(reader)
        except asyncio.IncompleteReadError:
          raise ConnectionError("Worker disconnected (incomplete read)")

        msg_type = message.get("type")

        if msg_type == "result":
          self.primes.add(message["prime"])
          snapshot.save_inflight(worker_id, message["prime"])
          
        elif msg_type == "marker":
          await snapshot.save_snapshot(
              self.afs,
              SNAPSHOT_NAME,
              message["workerid"],
              message["snapshot_id"],
              message["state"]
          )
        
        elif msg_type == "reconnect":
          wid = message["workerid"]
          self.workers[wid] = writer
          print(f"[Coordinator] Worker {wid} RECONNECTED.")
          if wid in self.tasks_in_progress:
             original_chunk = self.tasks_in_progress[wid]
             task = {"type":"task", "chunk": original_chunk, "workerid": wid}
             await send_msg(writer, task)
             print(f"[Coordinator] Resent task to Worker {wid}")

        elif msg_type == "finish":
          print(f"Worker {worker_id} finished task")
          self.finished_tasks_count += 1
          if worker_id in self.tasks_in_progress:
            del self.tasks_in_progress[worker_id]
          break

    except (ConnectionError, EOFError, ConnectionResetError, asyncio.IncompleteReadError) as e:
      print(f"Worker {worker_id} (from {ad}) disconnected: {e}")
    except Exception as e:
      print(f"Error with worker {worker_id}: {e}")
    finally:
      if worker_id in self.workers:
        del self.workers[worker_id]
        
      #re-queue task
      if worker_id in self.tasks_in_progress:
        requeued_task = self.tasks_in_progress.pop(worker_id)
        self.tasks.insert(0, requeued_task)
        print(f"Re-queued task from disconnected worker {worker_id}")

      if not writer.is_closing():
        writer.close()
        await writer.wait_closed()

  async def run(self, input_path: str, output_path: str):
    await self.initialize_afs()
    numbers = await self.read_from_afs(input_path)
    self.tasks = self.split_numbers(numbers, NUM_WORKERS)
    self.total_tasks = len(self.tasks)
    self.finished_tasks_count = 0
    
    snapshot_task = asyncio.create_task(self.snapshot_loop())

    server = await asyncio.start_server(
        self.handle_worker, HOST, PORT,
        #reuse_address=True
    )
    addr = server.sockets[0].getsockname()
    print(f"Coordinator listening on {addr[0]}:{addr[1]}, waiting for workers.")
    print(f"Total tasks to complete: {self.total_tasks}")

    while self.finished_tasks_count < self.total_tasks:
      await asyncio.sleep(1)
      print(f"Progress: {self.finished_tasks_count} / {self.total_tasks} tasks done. "
            f"({len(self.tasks_in_progress)} in progress, {len(self.tasks)} queued, {len(self.workers)} workers)")
      
      if not self.workers and not self.tasks_in_progress and self.tasks:
          print("All workers disconnected, but tasks remain. Waiting for new workers.")

    print("All tasks finished.")

    server.close()
    await server.wait_closed()
    snapshot_task.cancel()
    print(f"Total primes found: {len(self.primes)}")
    await self.save_results_to_afs(output_path, self.primes)

async def main():
  input_path = "/primefinder/data/test1000.txt"
  output_path = "/primefinder/outputs/primes_distributed.txt"

  if "--input" in sys.argv:
    try:
      input_path = sys.argv[sys.argv.index("--input") + 1]
    except IndexError:
      print("Error: --input flag requires an argument")
      sys.exit(1)
  
  if "--output" in sys.argv:
    try:
      output_path = sys.argv[sys.argv.index("--output") + 1]
    except IndexError:
      print("Error: --output flag requires an argument")
      sys.exit(1)

  print(f"Coordinator starting with:")
  print(f"  Input AFS Path:  {input_path}")
  print(f"  Output AFS Path: {output_path}")

  afs_servers = ["127.0.0.1:8888" 
                 #"127.0.0.1:8889", 
                 #"127.0.0.1:8890"
                 ]
  coordinator = AFSCoordinator(afs_servers)
  try:
    await coordinator.run(input_path=input_path, output_path=output_path)
  except KeyboardInterrupt:
    pass
  except Exception as e:
    print(f"Coordinator error: {e}")

if __name__ == "__main__":
  asyncio.run(main())