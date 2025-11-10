"""
Worker for finding prime numbers(stateless)
"""
import pickle
import sys
import asyncio
from primefinder.prime import is_prime

HOST = 'localhost'
PORT = 5000

async def worker(worker_id_str):
  reader: asyncio.StreamReader = None
  writer: asyncio.StreamWriter = None
  try:
    reader, writer = await asyncio.open_connection(HOST, PORT)
    print(f"[Worker {worker_id_str}] Connected to coordinator")
  except Exception as e:
    print(f"[Worker {worker_id_str}] Cannot connect to coordinator: {e}")
    return

  try:
    data = await reader.read(8192)
    task = pickle.loads(data)
    numbers = task["chunk"]
    workerid = task["workerid"]
    print(f"[Worker {workerid}] Received task with {len(numbers)} numbers")
    numid = 0
    found = []
    seen_marker = {}
    while numid < len(numbers):
      try:
        #listen marker
        if_marker = await asyncio.wait_for(reader.read(4096), timeout=0.001)
        if if_marker:
          message = pickle.loads(if_marker)
          if message.get("type") == "marker":
            sid = message["snapshot_id"]
            if not seen_marker.get(sid, False):
              seen_marker[sid] = True
              state = {
                "workerid": workerid,
                "numid": numid,
                "found_count": len(found)
              }
              reponse = pickle.dumps({
                "type": "marker",
                "snapshot_id": sid,
                "state": state
              })
              writer.write(reponse)
              await writer.drain()
              print(f"[Worker {workerid}] Sent marker response {sid}")
      except asyncio.TimeoutError:
        pass 
      except Exception as e:
        print (f"[Worker {workerid}] Error checking for marker: {e}")

      n = numbers[numid]
      if is_prime(n):
        found.append(n)
        result = pickle.dumps({"type": "result", "prime": n, "workerid": workerid})
        writer.write(result)
        await writer.drain()
          
      numid += 1
      await asyncio.sleep(0.003)

    finish = pickle.dumps({"type": "finish", "workerid": workerid})
    writer.write(finish)
    await writer.drain()
    print(f"[Worker {workerid}] Finished task")
  except (EOFError, ConnectionResetError) as e:
    print(f"[Worker {worker_id_str}] Coordinator disconnected: {e}")
  except Exception as e:
    print(f"[Worker {worker_id_str}] Error: {e}")
  finally:
    if writer:
      writer.close()
      await writer.wait_closed()
           
async def main(worker_id_str):
  await worker(worker_id_str)

if __name__ == "__main__":
  if len(sys.argv) < 2:
    sys.exit(1)
  worker_id_str = sys.argv[1]
  asyncio.run(main(worker_id_str))
