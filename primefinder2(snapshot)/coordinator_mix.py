import socket
import pickle
import math
import time
import threading
import os
from send_recv import send_data, recv_data
from snapshot import save_snapshot, load_snapshot

HOST = 'localhost'
PORT = 5000
NUM_WORKERS = 4
SNAPSHOT_FILE = "coordinator_snapshot.pkl"

def read_numbers(file_path):
    with open(file_path, 'r') as f:
        return [int(line.strip()) for line in f if line.strip()]


def split_numbers(num_list, n_workers):
    chunk_size = math.ceil(len(num_list) / n_workers)
    return [num_list[i:i + chunk_size] for i in range(0, len(num_list), chunk_size)]



def coordinator_main():
    snapshot = load_snapshot(SNAPSHOT_FILE)
    if snapshot is None:
        numbers = read_numbers("data/test1.txt")
        chunks = split_numbers(numbers, NUM_WORKERS)
        tasks = [{'id': i, 'data': chunk, 'status': 'pending', 'assigned_worker': None} for i, chunk in enumerate(chunks)]
        primes = set()
        print(f"[Coordinator] Starting fresh with {len(tasks)} chunks")
    else:
        tasks = snapshot['tasks']
        primes = set(snapshot['primes'])
        print(f"[Coordinator] Restored snapshot: {sum(t['status'] == 'done' for t in tasks)}/{len(tasks)} done")

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen()
    print(f"[Coordinator] Listening on {HOST}:{PORT} ...")

    workers = {}
    lock = threading.Lock()

    # heartbeats tracker
    def heartbeat_tracker():
        while True:
            time.sleep(3)
            now = time.time()
            with lock:
                for wid, info in list(workers.items()):
                    if info["alive"] and now - info["last_heartbeat"] > 10:
                        print(f"[Warning] Worker {wid} timeout, marking as dead.")
                        info["alive"] = False
                        for t in tasks:
                            if t["assigned_worker"] == wid and t["status"] != "done":
                                t["status"] = "pending"
                                t["assigned_worker"] = None

    threading.Thread(target= heartbeat_tracker, daemon=True).start()

    completed = sum(1 for t in tasks if t["status"] == "done")

    while completed < len(tasks):
        server.settimeout(2.0)
        try:
            conn, addr = server.accept()
            wid = f"worker_{len(workers) + 1}"
            workers[wid] = {"conn": conn, "addr": addr, "alive": True, "last_heartbeat": time.time()}
            print(f"[Coordinator] New worker connected: {wid} from {addr}")
        except socket.timeout:
            pass

        for wid, info in list(workers.items()):
            conn = info["conn"]
            try:
                conn.settimeout(1)
                msg = recv_data(conn)
                if not msg:
                    continue

                msg_type = msg.get("type")

                if msg_type == "HEARTBEAT":
                    info["last_heartbeat"] = time.time()

                elif msg_type == "RESULT":
                    task_id = msg["task_id"]
                    data = msg["data"]
                    primes.update(data)
                    tasks[task_id]["status"] = "done"
                    tasks[task_id]["assigned_worker"] = None
                    send_data(conn, {"type": "ACK"})
                    print(f"[Coordinator] Received result for task {task_id} from {wid} ({len(data)} primes).")

                    completed = sum(1 for t in tasks if t["status"] == "done")

                    #update snapshot
                    save_snapshot(SNAPSHOT_FILE, {"tasks": tasks, "primes": list(primes)})

            except socket.timeout:
                continue
            except Exception:
                continue

        # deliver new task
        for t in tasks:
            if t["status"] == "pending":
                # find free worker
                for wid, info in workers.items():
                    if info["alive"]:
                        try:
                            send_data(info["conn"], {"type": "TASK", "task_id": t["id"], "data": t["data"]})
                            t["status"] = "running"
                            t["assigned_worker"] = wid
                            print(f"[Coordinator] Assigned task {t['id']} to {wid}")
                            save_snapshot(SNAPSHOT_FILE, {"tasks": tasks, "primes": list(primes)})
                            break
                        except Exception as e:
                            info["alive"] = False
                            print(f"[Coordinator] Failed to send task {t['id']} to {wid}: {e}")

        time.sleep(0.5)
    print(f"[Coordinator] All tasks completed! Total primes: {len(primes)}")

    with open("outputs/primes_distributed.txt", "w") as f:
        for p in sorted(primes):
            f.write(f"{p}\n")

    if os.path.exists(SNAPSHOT_FILE):
        os.remove(SNAPSHOT_FILE)
        print("[Coordinator] Snapshot cleaned up")

    server.close()
    print("[Coordinator] Finished.")


if __name__ == "__main__":
    coordinator_main()
