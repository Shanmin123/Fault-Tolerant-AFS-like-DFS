import socket
import pickle
import threading
import time
import random
import os
import sys
from prime import is_prime
from woker import save_snapshot, load_snapshot
from send_recv import send_data, recv_data

HOST = 'localhost'
PORT = 5000

def heartbeat(conn,worker_id):
    while True:
        time.sleep(1)
        try:
            send_data(conn, {'type': 'HEARTBEAT','worker_id':worker_id})
        except Exception as e:
            print(e)
            break

def worker_main(worker_id):
    SNAPSHOT_FILE = f"worker_snapshot_{worker_id}"

    state = load_snapshot(SNAPSHOT_FILE)
    if state:
        task_id = state.get("task_id")
        numbers = state.get("data", [])
        processed_count = state.get("processed_count", 0)
        found_primes = set(state.get("found_primes", []))
        print(f"[Worker {worker_id}] Resuming from snapshot: {processed_count}/{len(numbers)} processed")
    else:
        task_id = None
        numbers = []
        processed_count = 0
        found_primes = set()
    #connect to coordinator
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((HOST, PORT))
        print(f"[Worker {worker_id}] Connected to coordinator")
    except (ConnectionRefusedError, ConnectionResetError):
        print(f"[Worker {worker_id}] Cannot connect to coordinator, exiting")
        return False

    #start heartbeat:
    threading.Thread(target=heartbeat, args=(s,worker_id),daemon=True).start()

    #if no task data, get new task
    if not numbers:
        print("[Worker {worker_id}] wait for new task")
        msg=recv_data(s)
        if not msg:
            print(f"[Worker {worker_id}] no message received")
            return False
        if msg['type'] == 'NO_TASK':
            print(f"[Worker {worker_id}] no new task")
            s.close()
            return False
        if msg.get("type") == "TASK":
            task_id = msg["task_id"]
            numbers = msg["data"]
            processed_count = 0
            found_primes = set()
            save_snapshot(SNAPSHOT_FILE, {
                "task_id": task_id,
                "data": numbers,
                "processed_count": 0,
                "found_primes": []
            })
            print(f"[Worker {worker_id}] Received task {task_id} with {len(numbers)} numbers.")

    print(f"[Worker {worker_id}] Processing from index {processed_count}...")

    try:
        for i in range(processed_count, len(numbers)):
            n = numbers[i]
            time.sleep(0.05)

            if random.random() < 0.003:
                print(f"[Worker {worker_id}] simulated crash at number {n} (index {i})")

                save_snapshot(SNAPSHOT_FILE, {
                    "task_id": task_id,
                    "data": numbers,
                    "processed_count": i,
                    "found_primes": list(found_primes)
                })
                s.close()
                os._exit(1)

            #prime test
            if is_prime(n):
                found_primes.add(n)

            processed_count = i + 1

            #each snapshot takes after 10 prime tests
            if processed_count % 10 == 0:
                save_snapshot(SNAPSHOT_FILE, {
                    "task_id": task_id,
                    "data": numbers,
                    "processed_count": processed_count,
                    "found_primes": list(found_primes)
                })
                print(f"[Worker {worker_id}] Progress: {processed_count}/{len(numbers)}")
        result_msg = {'type': 'RESULT','task_id':task_id ,'data': list(found_primes)}
        send_data(s,result_msg)
        print(f"[Worker {worker_id}] Sent results: {len(found_primes)} primes, {task_id}")

        ack = recv_data(s)
        if ack and ack.get("type") == "ACK":
            print(f"[Worker {worker_id}] Received ACK, task completed.")
            if os.path.exists(SNAPSHOT_FILE):
                os.remove(SNAPSHOT_FILE)
                print(f"[Worker {worker_id}] Snapshot deleted.")
        else:
            print(f"[Worker {worker_id}] Did not receive ACK.")

        s.close()
        return True

    except Exception as e:
        print(f"[Worker {worker_id}] Error during processing: {e}")
        save_snapshot(SNAPSHOT_FILE, {
            "task_id": task_id,
            "data": numbers,
            "processed_count": processed_count,
            "found_primes": list(found_primes)
        })
        s.close()
        return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python worker.py <worker_id>")
        sys.exit(1)

    worker_id = int(sys.argv[1])

    print(f"[Worker {worker_id}] Starting...")

    should_continue = worker_main(worker_id)

    if should_continue:

        SNAPSHOT_FILE = f"worker_snapshot_{worker_id}.pkl"
        if load_snapshot(SNAPSHOT_FILE.):
            print(f"[Worker {worker_id}] Task incomplete! Restart this worker to resume:")
            print(f"    python worker.py {worker_id}")
        else:
            print(f"[Worker {worker_id}] Task completed successfully")
    else:
        print(f"[Worker {worker_id}] No more tasks available")
