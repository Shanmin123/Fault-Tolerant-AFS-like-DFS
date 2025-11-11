import sys
import os
import asyncio
import pickle

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from primefinder.prime import is_prime

HOST = "127.0.0.1"
PORT = 5000

async def send_msg(writer: asyncio.StreamWriter, obj) -> None:
    data = pickle.dumps(obj)
    writer.write(len(data).to_bytes(4, "big") + data)
    await writer.drain()

async def recv_msg(reader: asyncio.StreamReader):
    header = await reader.readexactly(4)
    n = int.from_bytes(header, "big")
    payload = await reader.readexactly(n)
    return pickle.loads(payload)

async def worker(worker_id_str: str):
    reader: asyncio.StreamReader = None
    writer: asyncio.StreamWriter = None
    try:
        reader, writer = await asyncio.open_connection(HOST, PORT)
        print(f"[Worker {worker_id_str}] Connected to coordinator")
    except Exception as e:
        print(f"[Worker {worker_id_str}] Cannot connect to coordinator: {e}")
        return
    try:
        task = await recv_msg(reader)
        numbers = task["chunk"]
        workerid = task.get("workerid", worker_id_str)
        print(f"[Worker {workerid}] Received task with {len(numbers)} numbers")
        numid = 0
        found = []
        seen_marker = {}
        while numid < len(numbers):
            try:
                msg = await asyncio.wait_for(recv_msg(reader), timeout=0.001)
            except asyncio.TimeoutError:
                msg = None
            except asyncio.IncompleteReadError:
                raise ConnectionError("Coordinator disconnected during processing")
            if msg and msg.get("type") == "marker":
                sid = msg["snapshot_id"]
                if sid not in seen_marker:
                    seen_marker[sid] = True
                    state = {"workerid": workerid, "numid": numid, "found_count": len(found)}
                    await send_msg(writer, {"type": "marker", "snapshot_id": sid, "state": state, "workerid": workerid})
                    print(f"[Worker {workerid}] Sent marker response for snapshot {sid}")
            n = numbers[numid]
            if is_prime(n):
                found.append(n)
                await send_msg(writer, {"type": "result", "prime": n, "workerid": workerid})
            numid += 1
            await asyncio.sleep(0.003)
        await send_msg(writer, {"type": "finish", "workerid": workerid})
        print(f"[Worker {workerid}] Finished task, found {len(found)} primes")
    except (EOFError, ConnectionResetError, asyncio.IncompleteReadError) as e:
        print(f"[Worker {worker_id_str}] Coordinator disconnected: {e}")
    except Exception as e:
        print(f"[Worker {worker_id_str}] Error: {e}")
        import traceback; traceback.print_exc()
    finally:
        if writer:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

async def main(worker_id_str: str):
    await worker(worker_id_str)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m primefinder.worker <worker_id>")
        print("Example: python -m primefinder.worker 1")
        sys.exit(1)
    asyncio.run(main(sys.argv[1]))