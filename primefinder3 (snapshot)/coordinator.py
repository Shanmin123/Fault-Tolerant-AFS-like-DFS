import socket
import pickle
import math
import threading

import snapshot
import time

HOST = 'localhost'
PORT = 5000
NUM_workers = 4


def readFile(file_path):
    with open(file_path, 'r') as f:
        nums = []
        for line in f:
            num = int(line)
            nums.append(num)
        return nums

def split(nums, n):
    chunk_size = math.ceil(len(nums)/n)
    chunks = []
    i = 0
    while i < len(nums):
        chunk = nums[i:i + chunk_size]
        chunks.append(chunk)
        i += chunk_size
    return chunks

finished =set()
def recieve(workerid, conn, primes, snap):
    while True:
        data = conn.recv(4096)
        # if not data:
        #     break
        message = pickle.loads(data)
        t = message.get("type")
        if t == "result":
            primes.add(message["prime"])
            snapshot.save_inflight(workerid, message["prime"])
        elif t == "marker":
            snapshot.save_snapshot(workerid, message["snapshot_id"], message["state"])
        elif t == "finish":
            print("Worker", workerid, "finished")
            finished.add(workerid)
            break

def snapshot_loop(workers, snapshot, primes):
    while True:
        time.sleep(1)
        sid = snapshot.start(len(primes))
        if sid:
            for conn in workers:
                conn.sendall(pickle.dumps({"type": "marker", "snapshot_id": sid}))


def coordinator():
    numbers = readFile("data/test10000.txt")
    chunks = split(numbers, NUM_workers)
    primes = set()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen()

    print(f"Coordinator started on {HOST}:{PORT}, waiting for {NUM_workers} workers...")

    workers = []
    while len(workers) < NUM_workers:
        conn, addr = server.accept()
        print(f"Worker connected from {addr}")
        workers.append(conn)

    for i, conn in enumerate(workers):
        task = {"type":"task", "chunk":chunks[i], "workerid": i+1}
        task = pickle.dumps(task)
        conn.sendall(task)

    snapshot.init(NUM_workers)

    for i, conn in enumerate(workers, start=1):
        t = threading.Thread(
            target=recieve,
            args=(i, conn, primes, snapshot)
        )
        t.start()

    t= threading.Thread(
        target=snapshot_loop,
        args=(workers, snapshot, primes)
    )
    t.start()

    while True:
        time.sleep(1)
        if len(finished) == NUM_workers:
            with open("outputs/primesResult.txt", "w") as f:
                for p in primes:
                    f.write(f"{p}\n")

            print("finished")
            break


if __name__ == "__main__":
    coordinator()