
import pickle
import time


snapshot_id = 0
active = False
NUM_workers = 4
marker_received = {}
inflight = {}
worker_state = {}
local_state = {}

def init(n):
    global NUM_workers
    NUM_workers = n

def start(collected_count):
    global snapshot_id, active, marker_received, inflight, worker_state, local_state
    if active == True:
        return None
    snapshot_id += 1
    active = True
    for i in range (NUM_workers):
        marker_received[i+1] = False
        inflight[i+1] = []
    worker_state = {}
    local_state = {"time": time.time(), "collected": collected_count}
    print(f"start{snapshot_id} ")
    return snapshot_id

def save_inflight(wokerid, prime):
    if active and not marker_received.get(wokerid, False):
        inflight[wokerid].append(prime)

def save_snapshot(workerid, sid, state):
    global active
    if active and sid == snapshot_id and not marker_received[workerid]:
        worker_state[workerid] = state
        marker_received[workerid] = True
        if all(marker_received.values()) == True:
            data = {
                "snapshot_id": snapshot_id,
                "time": time.time(),
                "coordinator_state": local_state,
                "worker_state": worker_state,
                "inflight": inflight
            }
            path = f"snapshots/snapshot_{snapshot_id}.pkl"
            with open(path, "wb") as f:
                pickle.dump(data, f)
            print(f"{snapshot_id}saved")
            active = False


