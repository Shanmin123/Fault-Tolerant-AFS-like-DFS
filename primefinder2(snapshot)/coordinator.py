import socket
import pickle
import math
import os
import time
from snapshot import save_snapshot, load_snapshot

HOST = 'localhost'
PORT = 5000
NUM_workers = 4
SNAPSHOT_FILE = "coordinator_snapshot"


def read_numbers(file_path):
    with open(file_path, 'r') as f:
        return [int(line.strip()) for line in f if line.strip()]


def split_numbers(num, n_workers):
    chunk_size = math.ceil(len(num) / n_workers)
    return [num[i:i + chunk_size] for i in range(0, len(num), chunk_size)]


def coordinator_main():

    snapshot = load_snapshot(SNAPSHOT_FILE)
    if snapshot is None:
        numbers = read_numbers("data/test1000.txt")
        remaining_chunks = split_numbers(numbers, NUM_workers)
        done_chunks = []
        primes = set()
        print(f"[Coordinator] Starting fresh with {len(remaining_chunks)} chunks")
    else:
        remaining_chunks = snapshot['remaining_chunks']
        done_chunks = snapshot['done_chunks']
        primes = set(snapshot['primes'])
        print(f"[Coordinator] Restored snapshot: {len(remaining_chunks)} chunks remain, {len(done_chunks)} done")

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen()
    print(f"Coordinator started on {HOST}:{PORT}, waiting for workers...")

    active_workers = []

    while remaining_chunks or active_workers:

        if remaining_chunks and len(active_workers) < NUM_workers:
            server.settimeout(2.0)
            try:
                conn, addr = server.accept()
                print(f"[Coordinator] Worker connected from {addr}")

                chunk = remaining_chunks.pop(0)
                chunk_id = hash(tuple(chunk))
                task = {"type": "task", "chunk_id": chunk_id, "data": chunk}
                conn.sendall(pickle.dumps(task))

                active_workers.append((conn, chunk_id, chunk))
                print(f"[Coordinator] Assigned chunk {chunk_id} ({len(chunk)} numbers)")

                save_snapshot(SNAPSHOT_FILE, {
                    "remaining_chunks": remaining_chunks,
                    "done_chunks": done_chunks,
                    "primes": list(primes)
                })
            except socket.timeout:
                pass


        for i in range(len(active_workers) - 1, -1, -1):
            conn, chunk_id, chunk = active_workers[i]
            conn.settimeout(0.5)

            try:
                data = conn.recv(4096)
                if data:
                    result = pickle.loads(data)

                    primes.update(result["primes"])
                    done_chunks.append(result["chunk_id"])

                    print(f"[Coordinator] Received result for chunk {chunk_id}: {len(result['primes'])} primes")

                    conn.sendall(b"ok")
                    conn.close()


                    active_workers.pop(i)


                    save_snapshot(SNAPSHOT_FILE, {
                        "remaining_chunks": remaining_chunks,
                        "done_chunks": done_chunks,
                        "primes": list(primes)
                    })
            except socket.timeout:
                continue
            except (ConnectionResetError, ConnectionAbortedError, EOFError) as e:
                print(f"[Coordinator] Worker disconnected for chunk {chunk_id}, re-queuing...")

                remaining_chunks.append(chunk)
                conn.close()
                active_workers.pop(i)


                save_snapshot(SNAPSHOT_FILE, {
                    "remaining_chunks": remaining_chunks,
                    "done_chunks": done_chunks,
                    "primes": list(primes)
                })


        if not remaining_chunks and not active_workers:
            break

        time.sleep(0.1)

    print(f"[Coordinator] All tasks completed! Total primes: {len(primes)}")
    with open("outputs/primes_distributed2.txt", "w") as f:
        for p in sorted(primes):
            f.write(f"{p}\n")


    snapshot_path = f"snapshots/{SNAPSHOT_FILE}.pkl"
    if os.path.exists(snapshot_path):
        os.remove(snapshot_path)
        print("[Coordinator] Snapshot cleaned up")

    server.close()
    print("[Coordinator] Finished!")


if __name__ == "__main__":
    coordinator_main()