"""
Chandy-Lamport snapshot afs integrated
"""
import pickle
import time
from AFS.afs.client import AFSClient

snapshot_id = 0
active = False
NUM_workers = 4
marker_received = {}
inflight = {}
worker_state = {}
local_state = {}
SNAPSHOT_LATEST_AFS_PATH = "snapshots/snapshot_latest.pkl"

def init(n):
    global NUM_workers
    NUM_workers = n

def start(coordinator_state_snapshot: dict):
    global snapshot_id, active, marker_received, inflight, worker_state, local_state
    if active == True:
        return None
    snapshot_id += 1
    active = True
    for i in range (NUM_workers):
        marker_received[i+1] = False
        inflight[i+1] = []
    worker_state = {}
    local_state = coordinator_state_snapshot
    local_state["snapshot_time"] = time.time()
    print(f"start{snapshot_id} ")
    return snapshot_id

def save_inflight(wokerid, prime):
    if active:
        for i in inflight:
            if not marker_received.get(i, False):
                if not marker_received.get(wokerid, False):
                  inflight[wokerid].append(prime)
                  break

#afs-snapshot
async def save_snapshot(afs_client: AFSClient, snapshot_name: str, workerid, sid, state):
    #save worker state, if all ack, save to afs
    global active, worker_state, marker_received
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

            path = f"snapshots/{snapshot_name}_{snapshot_id}.pkl"
            path_latest = f"snapshots/snapshot_latest.pkl"
            try:
                bytes = pickle.dumps(data)
                #to afs
                try:
                    fd = await afs_client.create(path)
                except:
                    #already exist
                    fd = await afs_client.open(path, "w")
                afs_client.write(fd, bytes)
                await afs_client.close(fd)

                try:
                    fd_latest = await afs_client.create(path_latest)
                except:
                    fd_latest = await afs_client.open(path_latest, mode="w")
                afs_client.write(fd_latest, bytes)
                await afs_client.close(fd_latest)

                print(f"[Snapshot] Snapshot {snapshot_id} saved to AFS at {path}")
            except Exception as e:
                print(f"Fail to save{snapshot_id} to AFS: {e}")
            
            #restate
            active = False

