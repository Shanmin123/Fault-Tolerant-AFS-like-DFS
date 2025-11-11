import socket
import pickle
import math
import threading
import select
import snapshot
import time
import os

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
def recieve(workerid, conn, primes, chunks):
    while True:
        data = conn.recv(4096)
        if not data:
            break
        message = pickle.loads(data)
        t = message.get("type")
        if t == "result":
            primes.add(message["prime"])
            snapshot.save_inflight(workerid, message["prime"])
        elif t == "marker":
            snapshot.save_snapshot(workerid, message["snapshot_id"], message["state"])
        elif t == "reconnect":
            print("resending task")
            workerid = message["workerid"]
            # chunk = chunks[workerid-1]
            task = {"type":"task", "chunk":chunks[workerid-1], "workerid": workerid}
            task = pickle.dumps(task)
            conn.sendall(task)
            print("resent task")
        elif t == "finish":
            print("Worker", workerid, "finished")
            finished.add(workerid)
            break

def snapshot_loop(workers,snapshot, primes):
    written_primes = set()
    output_path = snapshot.output_file

    if os.path.exists(output_path):
        written_primes = readFile(output_path)
        print(f"[SnapshotLoop] recovered {len(written_primes)} primes from {output_path}")

    while True:
        time.sleep(1)
        new_primes = primes - written_primes
        if new_primes:
            with open(output_path, "a") as f:
                for p in sorted(new_primes):
                    f.write(f"{p}\n")
            written_primes |= new_primes
            print(f"[SnapshotLoop] wrote {len(new_primes)} new primes to {output_path}")

        sid = (snapshot.start(len(primes)))
        if sid:
            for conn in workers:
                conn.sendall(pickle.dumps({"type": "marker", "snapshot_id": sid}))


def coordinator():
    input_file = input("enter the path of input file ").strip()
    output_file = input("enter the path of output file ").strip()

    numbers = readFile(input_file)
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

    snapshot.init(NUM_workers,input_file,output_file)

    for i, conn in enumerate(workers, start=1):
        t = threading.Thread(
            target=recieve,
            args=(i, conn, primes, chunks)
        )
        t.start()

    t= threading.Thread(
        target=snapshot_loop,
        args=(workers, snapshot, primes)
    )
    t.start()

    while True:
        readable, _, _ = select.select([server], [], [], 0.1)
        if server in readable:
            conn, addr = server.accept()
            print(f"[Coordinator] New connection from {addr}")
            t = threading.Thread(
                target=recieve,
                args=(0, conn, primes, chunks)
            )
            t.start()

        time.sleep(1)
        if len(finished) == NUM_workers:
            with open(output_file, "w") as f:
                for p in primes:
                    f.write(f"{p}\n")
            os.remove("snapshots/snapshot_latest.pkl")

            print("all task finished")

            break

    server.close()
    os._exit(0)


if __name__ == "__main__":
    coordinator()
