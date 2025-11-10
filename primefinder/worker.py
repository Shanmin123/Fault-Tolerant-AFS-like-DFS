"""
Worker for finding prime numbers (asyncio version)
"""
import pickle
import sys
import asyncio
import os

# Add project root to path if needed
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from primefinder.prime import is_prime

HOST = 'localhost'
PORT = 5000


async def worker(worker_id_str):
    """Worker main function using asyncio"""
    reader: asyncio.StreamReader = None
    writer: asyncio.StreamWriter = None

    try:
        # Connect to coordinator
        reader, writer = await asyncio.open_connection(HOST, PORT)
        print(f"[Worker {worker_id_str}] Connected to coordinator")
    except Exception as e:
        print(f"[Worker {worker_id_str}] Cannot connect to coordinator: {e}")
        return

    try:
        # Receive task from coordinator
        data = await reader.read(8192)
        task = pickle.loads(data)
        numbers = task["chunk"]
        workerid = task["workerid"]
        print(f"[Worker {workerid}] Received task with {len(numbers)} numbers")

        numid = 0
        found = []
        seen_marker = {}

        # Process numbers
        while numid < len(numbers):
            try:
                # Check for marker messages (non-blocking)
                marker_data = await asyncio.wait_for(reader.read(4096), timeout=0.001)
                if marker_data:
                    message = pickle.loads(marker_data)
                    if message.get("type") == "marker":
                        sid = message["snapshot_id"]
                        if sid not in seen_marker:
                            seen_marker[sid] = True
                            state = {
                                "workerid": workerid,
                                "numid": numid,
                                "found_count": len(found)
                            }
                            response = pickle.dumps({
                                "type": "marker",
                                "snapshot_id": sid,
                                "state": state,
                                "workerid": workerid
                            })
                            writer.write(response)
                            await writer.drain()
                            print(f"[Worker {workerid}] Sent marker response for snapshot {sid}")
            except asyncio.TimeoutError:
                # No marker received, continue processing
                pass
            except Exception as e:
                print(f"[Worker {workerid}] Error checking for marker: {e}")

            # Check if number is prime
            n = numbers[numid]
            if is_prime(n):
                found.append(n)
                result = pickle.dumps({"type": "result", "prime": n, "workerid": workerid})
                writer.write(result)
                await writer.drain()

            numid += 1
            await asyncio.sleep(0.003)  # Small delay to simulate work

        # Send finish message
        finish = pickle.dumps({"type": "finish", "workerid": workerid})
        writer.write(finish)
        await writer.drain()
        print(f"[Worker {workerid}] Finished task, found {len(found)} primes")

    except (EOFError, ConnectionResetError) as e:
        print(f"[Worker {worker_id_str}] Coordinator disconnected: {e}")
    except Exception as e:
        print(f"[Worker {worker_id_str}] Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if writer:
            writer.close()
            await writer.wait_closed()


async def main(worker_id_str):
    """Entry point for asyncio"""
    await worker(worker_id_str)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m primefinder.worker <worker_id>")
        print("Example: python -m primefinder.worker 1")
        sys.exit(1)

    worker_id_str = sys.argv[1]
    asyncio.run(main(worker_id_str))