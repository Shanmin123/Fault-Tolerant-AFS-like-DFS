
import pickle
import time
import threading


snapshot_id = 0
active = False
NUM_workers = 4
marker_received = {}
inflight = {}
worker_state = {}
local_state = {}

lock = threading.Lock()

def init(n):
    global NUM_workers
    with lock:
        NUM_workers = n

def start(collected_count):
    global snapshot_id, active, marker_received, inflight, worker_state, local_state
    with lock:
        if active == True:
            return None
        snapshot_id += 1
        active = True
        for i in range(NUM_workers):
            marker_received[i + 1] = False
            inflight[i + 1] = []
        worker_state = {}
        local_state = {"time": time.time(), "collected": collected_count}
        print(f"start{snapshot_id} ")
        return snapshot_id


def save_inflight(wokerid, prime):
    with lock:
        if active and not marker_received.get(wokerid, False):
            inflight[wokerid].append(prime)


def save_snapshot(workerid, sid, state):
    global active
    with lock:
        if active and sid == snapshot_id and not marker_received[workerid]:
            worker_state[workerid] = state
            marker_received[workerid] = True
            if all(marker_received.values()) == True:
                data = {
                    "snapshot_id": snapshot_id,
                    "time": time.time(),
                    "number of workers": NUM_workers,
                    "coordinator_state": local_state,
                    "worker_state": worker_state,
                    "inflight": inflight
                }
                path1 = f"snapshots/snapshot_{snapshot_id}.pkl"
                path2 = f"snapshots/snapshot_latest.pkl"
                with open(path1, "wb") as f:
                    pickle.dump(data, f)
                with open(path2, "wb") as f:
                    pickle.dump(data, f)
                print(f"{snapshot_id}saved")
                active = False



