import socket
import pickle
from prime import is_prime
import select
import time
HOST = 'localhost'
PORT = 5000

def worker():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.connect((HOST, PORT))
    print("Connected to coordinator")

    data = server.recv(4096)
    task = pickle.loads(data)
    numbers = task["chunk"]
    workerid = task["workerid"]

    numid = 0
    found = []
    seen_marker = {}

    while numid < len(numbers):

        readable, _, _ = select.select([server], [], [], 0)
        if server in readable:
            ctrl_data = server.recv(4096)
            if ctrl_data:
                message = pickle.loads(ctrl_data)
                if message.get("type") == "marker":
                    sid = message["snapshot_id"]
                    if not seen_marker.get(sid, False):
                        seen_marker[sid] = True
                        state = {

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