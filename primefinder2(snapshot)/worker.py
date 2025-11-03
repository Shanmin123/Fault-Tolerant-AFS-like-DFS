import socket
import pickle
import time
import random
import os
import sys
from prime import is_prime
from snapshot import save_snapshot, load_snapshot

HOST = 'localhost'
PORT = 5000


def worker_main(worker_id):
    SNAPSHOT_FILE = f"worker_snapshot_{worker_id}"

    state = load_snapshot(SNAPSHOT_FILE)
    if state:
        chunk_id = state.get("chunk_id")
        numbers = state.get("data", [])
        processed_count = state.get("processed_count", 0)
        found_primes = set(state.get("found_primes", []))
        print(f"[Worker {worker_id}] Resuming from snapshot: {processed_count}/{len(numbers)} processed")
    else:
        chunk_id = None
        numbers = []
        processed_count = 0
        found_primes = set()

    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((HOST, PORT))
        print(f"[Worker {worker_id}] Connected to coordinator")
    except (ConnectionRefusedError, ConnectionResetError):
        print(f"[Worker {worker_id}] Cannot connect to coordinator, exiting")
        return False

    if not numbers:
        try:
            data = s.recv(4096)
            task = pickle.loads(data)

            if task.get("type") == "no_task":
                print(f"[Worker {worker_id}] No more tasks available, exiting")
                s.close()
                return False  # 没有任务了

            chunk_id = task["chunk_id"]
            numbers = task["data"]
            processed_count = 0
            found_primes = set()
            save_snapshot(SNAPSHOT_FILE, {
                "chunk_id": chunk_id,
                "data": numbers,
                "processed_count": 0,
                "found_primes": []
            })
            print(f"[Worker {worker_id}] Received task: chunk_id={chunk_id}, {len(numbers)} numbers")
        except Exception as e:
            print(f"[Worker {worker_id}] Error receiving task: {e}")
            s.close()
            return False


    print(f"[Worker {worker_id}] Processing from index {processed_count}...")

    try:
        for i in range(processed_count, len(numbers)):
            n = numbers[i]


            time.sleep(0.05)


            if random.random() < 0.003:
                print(f"[Worker {worker_id}] simulated crash at number {n} (index {i})")

                save_snapshot(SNAPSHOT_FILE, {
                    "chunk_id": chunk_id,
                    "data": numbers,
                    "processed_count": i,
                    "found_primes": list(found_primes)
                })
                s.close()
                os._exit(1)


            if is_prime(n):
                found_primes.add(n)

            processed_count = i + 1

            if processed_count % 10 == 0:
                save_snapshot(SNAPSHOT_FILE, {
                    "chunk_id": chunk_id,
                    "data": numbers,
                    "processed_count": processed_count,
                    "found_primes": list(found_primes)
                })
                print(f"[Worker {worker_id}] Progress: {processed_count}/{len(numbers)}")
        result = {"chunk_id": chunk_id, "primes": list(found_primes)}
        s.sendall(pickle.dumps(result))
        print(f"[Worker {worker_id}] Sent results: {len(found_primes)} primes, {chunk_id}")


        ack = s.recv(1024)
        if ack == b"ok":
            print(f"[Worker {worker_id}] Received ACK, task completed")

            snapshot_path = f"snapshots/{SNAPSHOT_FILE}.pkl"
            if os.path.exists(snapshot_path):
                os.remove(snapshot_path)
                print(f"[Worker {worker_id}] Snapshot deleted")
            s.close()
            return True
        else:
            print(f"[Worker {worker_id}] Did not receive ACK")
            s.close()
            return True

    except Exception as e:
        print(f"[Worker {worker_id}] Error during processing: {e}")
        s.close()
        return True
    finally:
        s.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python worker.py <worker_id>")
        sys.exit(1)

    worker_id = int(sys.argv[1])

    print(f"[Worker {worker_id}] Starting...")

    should_continue = worker_main(worker_id)

    if should_continue:

        SNAPSHOT_FILE = f"worker_snapshot_{worker_id}"
        if load_snapshot(SNAPSHOT_FILE):
            print(f"[Worker {worker_id}] Task incomplete! Restart this worker to resume:")
            print(f"    python worker.py {worker_id}")
        else:
            print(f"[Worker {worker_id}] Task completed successfully")
    else:
        print(f"[Worker {worker_id}] No more tasks available")