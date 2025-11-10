"""
Chandy-Lamport snapshot with AFS integration
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


def init(n):
    """Initialize snapshot system with number of workers"""
    global NUM_workers
    NUM_workers = n


def start(collected_count):
    """Start a new snapshot"""
    global snapshot_id, active, marker_received, inflight, worker_state, local_state
    if active == True:
        return None
    snapshot_id += 1
    active = True
    for i in range(NUM_workers):
        marker_received[i + 1] = False
        inflight[i + 1] = []
    worker_state = {}
    local_state = {"time": time.time(), "collected": collected_count}
    print(f"[Snapshot] Starting snapshot {snapshot_id}")
    return snapshot_id


def save_inflight(workerid, prime):
    """Record in-flight messages for Chandy-Lamport algorithm"""
    if active:
        for i in inflight:
            if not marker_received.get(i, False):
                if not marker_received.get(workerid, False):
                    inflight[workerid].append(prime)
                    break


async def save_snapshot(afs_client: AFSClient, snapshot_name: str, workerid, sid, state):
    """Save snapshot to AFS when all workers have responded"""
    global active, worker_state, marker_received
    if active and sid == snapshot_id and not marker_received[workerid]:
        worker_state[workerid] = state
        marker_received[workerid] = True

        # Check if all workers have sent their marker response
        if all(marker_received.values()) == True:
            data = {
                "snapshot_id": snapshot_id,
                "time": time.time(),
                "coordinator_state": local_state,
                "worker_state": worker_state,
                "inflight": inflight
            }

            path = f"/snapshots/{snapshot_name}_{snapshot_id}.pkl"
            try:
                bytes_data = pickle.dumps(data)
                # Write to AFS
                try:
                    fd = await afs_client.create(path)
                except:
                    # File already exists
                    fd = await afs_client.open(path, "w")
                afs_client.write(fd, bytes_data)
                await afs_client.close(fd)
                print(f"[Snapshot] Snapshot {snapshot_id} saved to AFS at {path}")
            except Exception as e:
                print(f"[Snapshot] Failed to save snapshot {snapshot_id} to AFS: {e}")

            # Reset state for next snapshot
            active = False

