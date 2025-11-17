"""
Worker for finding prime numbers
"""
import pickle
import sys
import asyncio
from primefinder.prime import is_prime
from AFS.rpc.client import RPCClient
from AFS.afs.client import AFSClient

#message framing
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
PORT = 5001
SNAPSHOT_LATEST = "snapshots/snapshot_latest.pkl"
#safety margin for state recovery
RECOVERY_REWIND_COUNT = 100 

class AFSWorker:
  def __init__(self, worker_id, afs_servers):
    self.worker_id = worker_id
    self.afs_servers = afs_servers
    self.afs: AFSClient = None
  
  async def initialize_afs(self):
    rpc = RPCClient(self.afs_servers, retries=2)
    self.afs = AFSClient(rpc=rpc)

  async def load_snapshot(self):
    #read latest snapshot from afs
    try:
      try:
        fd = await self.afs.open(SNAPSHOT_LATEST, mode="r")
      except Exception:
          print(f"[Worker {self.worker_id}] No latest snapshot found.")
          return (None, 0)
      
      content = await self.afs.read(fd)
      await self.afs.close(fd)
      
      if not content:
        return (None, 0)
      snap = pickle.loads(content)
      worker_state = snap.get("worker_state", {})
      
      worker_id_str = str(self.worker_id)
      worker_id_int = int(self.worker_id)

      state_to_use = None
      if worker_id_str in worker_state:
          state_to_use = worker_state[worker_id_str]
      elif worker_id_int in worker_state:
          state_to_use = worker_state[worker_id_int]

      if state_to_use:
        numid = state_to_use.get("numid", 0)
        task_id = state_to_use.get("task_id")
        print(f"[Worker {self.worker_id}] Found snapshot state, recover from index {numid}")
        return (task_id, numid)
      
      return (None, 0)
    except Exception as e:
      print(f"[Worker {self.worker_id}] Snapshot check failed (continuing as new): {e}")
      return (None, 0)

  async def message_receiver(self, reader: asyncio.StreamReader, marker_queue: asyncio.Queue, task_queue: asyncio.Queue ):
    try:
      while True:
        msg = await recv_msg(reader) 
        msg_type = msg.get("type")
        if msg_type == "marker":
          await marker_queue.put(msg)
        elif msg_type == "task" or msg_type == "shutdown":
          await task_queue.put(msg)
        else:
          print(f"[Worker {self.worker_id}] Receiver task received unknown msg: {msg_type}")
    except (asyncio.IncompleteReadError, ConnectionError, EOFError):
      print(f"  [Worker {self.worker_id}] Receiver task disconnected.")
    except Exception as e:
      print(f"[Worker {self.worker_id}] Receiver task error: {e}")

  async def run(self):
    await self.initialize_afs()
    
    saved_task_id, snapshot_numid = await self.load_snapshot()
    start_numid = max(0, snapshot_numid - RECOVERY_REWIND_COUNT)
    if snapshot_numid > 0:
      print(f"[Worker {self.worker_id}] Recovered from {snapshot_numid}, rewinding to {start_numid} for safety.")

    #connect to coor
    reader: asyncio.StreamReader = None
    writer: asyncio.StreamWriter = None
    try:
      reader, writer = await asyncio.open_connection(HOST, PORT)
      print(f"[Worker {self.worker_id}] Connected to coordinator")
      #send id to coor not receive allocation
      hello_msg = {"type": "hello", "workerid": self.worker_id}
      await send_msg(writer, hello_msg)
    except Exception as e:
      err_str = str(e)
      if "Connect call failed" in err_str or "Connection refused" in err_str or "[Errno 111]" in err_str or "[Errno 61]" in err_str:
          raise ConnectionRefusedError("Coordinator unreachable")
      else:
          print(f"Connection failed with unexpected error: {e}")
          return # 或者 raise
    
    marker_queue = asyncio.Queue()
    task_queue = asyncio.Queue()
    receiver_task = asyncio.create_task(
      self.message_receiver(reader, marker_queue, task_queue)
    )


    try:
      """
      #re-> reconnect
      if snapshot_numid > 0:
        msg = {
          "type": "reconnect", 
          "workerid": self.worker_id, 
          "numid": start_numid
        }
        await send_msg(writer, msg)
        print(f"[Worker {self.worker_id}] Sent reconnect message")
      """
      #task
      print(f"[Worker {self.worker_id}] Waiting for task...")
      try:
        task = await asyncio.wait_for(task_queue.get(), timeout=5.0)
      except asyncio.TimeoutError:
        raise SystemExit("JobFinished")
      

      if not task:
        print(f"[Worker {self.worker_id}] Did not receive valid task.")
        return
      if task.get("type") == "shutdown":
        print(f"[Worker {self.worker_id}] Received shutdown signal from coordinator. Exiting.")
        raise SystemExit("JobFinished")
      if task.get("type") != "task":
        print(f"[Worker {self.worker_id}] Did not receive valid task.")
        return
      numbers = task["chunk"]
      current_task_id = task.get("task_id")
      start_numid = 0
      
      if current_task_id and current_task_id == saved_task_id:
        start_numid = max(0, snapshot_numid - RECOVERY_REWIND_COUNT)
        print(f"[Worker {self.worker_id}] Task ID {current_task_id} matches snapshot. Recovering from {snapshot_numid}, rewinding to {start_numid}.")
      elif saved_task_id:
        print(f"[Worker {self.worker_id}] New task {current_task_id}. Starting from 0.")
      else:
        print(f"[Worker {self.worker_id}] New task {current_task_id}. Starting from 0.")
      print(f"[Worker {self.worker_id}] Processing chunk size: {len(numbers)}")

      numid = 0 
      found = []
      seen_marker = {}
      processed_since_yield = 0
      
      while numid < len(numbers):
        #skip work we've already done (from snapshot)
        if numid < start_numid:
          numid += 1
          continue

        #simulate crash
        #0.05% crash
        #if random.random() < 0.0005:
        #  print(f"[Worker {self.worker_id}] SIMULATING CRASH at index {numid}")
        #  return

        try:
          message = marker_queue.get_nowait()
          
          if message and message.get("type") == "marker":
              sid = message["snapshot_id"]
              if not seen_marker.get(sid, False):
                  seen_marker[sid] = True
                  state = {
                    "workerid": self.worker_id,
                    "numid": numid,
                    "found_count": len(found),
                    "task_id": current_task_id
                  }
                  marker_resp = {
                    "type": "marker",
                    "snapshot_id": sid,
                    "state": state,
                    "workerid": self.worker_id
                  }
                  await send_msg(writer, marker_resp)
                  print(f"[Worker {self.worker_id}] Sent marker response for {sid}")
        except asyncio.QueueEmpty:
           pass
        except asyncio.TimeoutError:
          pass
        except Exception as e:
          print(f"Error reading for marker: {e}")
          break

        n = numbers[numid]
        is_n_prime = await asyncio.to_thread(is_prime, n)
        if is_n_prime:
          found.append(n)
          result = {"type": "result", "prime": n, "workerid": self.worker_id}
          await send_msg(writer, result)
            
        numid += 1
        processed_since_yield += 1
        
        #yield to event loop every 100 numbers
        #if processed_since_yield >= 100:
        #  await asyncio.sleep(0)
        #  processed_since_yield = 0
        await asyncio.sleep(0.005)
      finish = {"type": "finish", "workerid": self.worker_id}
      await send_msg(writer, finish)
      print(f"[Worker {self.worker_id}] Finished task")
      
    except (ConnectionError, EOFError, ConnectionResetError, asyncio.IncompleteReadError) as e:
      print(f"[Worker {self.worker_id}] Coordinator disconnected: {e}")
    except Exception as e:
      print(f"[Worker {self.worker_id}] Error: {e}")
      import traceback; traceback.print_exc()
    finally:
      if 'receiver_task' in locals() and not receiver_task.done():
          receiver_task.cancel()
      
      if writer:
        writer.close()
        await writer.wait_closed()


async def main(worker_id):
  afs_servers = ["127.0.0.1:8888", "127.0.0.1:8889", "127.0.0.1:8890"]
  
  #loop to keep working until no tasks are left
  while True:
    worker = AFSWorker(worker_id, afs_servers)
    try:
      await worker.run()
    except SystemExit as e:
      if str(e) == "JobFinished":
        print(f"[Worker {worker_id}] Job is finished. Worker process is exiting.")
        break
      else:
        raise
    except ConnectionRefusedError:
      print(f"[Worker {worker_id}] Connection refused. Assuming job is done. Exiting.")
      break
    except Exception as e:
      print(f"[Worker {worker_id}] Main run failed with: {e}")
    print(f"[Worker {worker_id}] Re-connecting for new task in 3 seconds...")
    await asyncio.sleep(3)


if __name__ == "__main__":
  if len(sys.argv) < 2:
    sys.exit(1)
  
  worker_id = sys.argv[1]
  try:
      worker_id_int = int(worker_id)
  except ValueError:
      print("Error: Worker ID must be an integer.")
      sys.exit(1)
  asyncio.run(main(worker_id_int))