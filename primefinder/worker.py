import socket
import pickle
import sys

from prime import is_prime
import select
import time
import os
HOST = 'localhost'
PORT = 5000
snapshot_path = f"snapshots/snapshot_latest.pkl"

def load_snapshot(workerid):
    if not os.path.exists(snapshot_path):
        return 0
    f = open(snapshot_path, "rb")
    snap = pickle.load(f)
    f.close()
    worker_state = snap.get("worker_state", {})
    if workerid in worker_state:
        numid = worker_state[workerid]["numid"]
        print("worker is recovering", "worker", workerid, "starts from ", numid,"th number in the chunk")

        return numid
    return 0

def worker():
    workerid = int(sys.argv[1])
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.connect((HOST, PORT))
    print(f"woker{workerid} connected to coordinator")

    numid = load_snapshot(workerid)
    if numid != 0:

        message = {"type": "reconnect", "workerid": workerid, "numid": numid}
        server.sendall(pickle.dumps(message))
        print("reconnect message is sent")

    data = server.recv(4096)

    task = pickle.loads(data)
    numbers = task["chunk"]


    found = []
    seen_marker = {}

    while numid < len(numbers):

        readable, _, _ = select.select([server], [], [], 0)
        if server in readable:
            data = server.recv(4096)
            if data:
                message = pickle.loads(data)
                if message["type"] == "marker":
                    sid = message["snapshot_id"]
                    if sid not in seen_marker:
                        seen_marker[sid] = True
                        state = {
                            "time": time.time(),

                            "workerid": workerid,
                            "numid": numid,
                            "found_count": len(found)
                        }
                        server.sendall(pickle.dumps({
                            "type": "marker",
                            "snapshot_id": sid,
                            "state": state
                        }))
                        print("Worker", workerid, "sent marker", sid)

        n = numbers[numid]
        if is_prime(n):
            found.append(n)
            server.sendall(pickle.dumps({"type": "result", "prime": n, "workerid": workerid}))
        numid += 1
        time.sleep(0.003)

    server.sendall(pickle.dumps({"type": "finish", "workerid": workerid}))
    server.close()
    print("Worker", workerid, "finish")



    # primes = set()
    # for i in numbers:
    #     if is_prime(i):
    #         primes.add(i)
    #
    #
    # server.sendall(pickle.dumps(primes))
    # server.close()

if __name__ == "__main__":
    worker()
