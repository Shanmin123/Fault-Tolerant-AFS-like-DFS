# DS-2025-Group-coursework1

This is a distributed systems project composed of two main parts:
1. AFS/Raft: A distributed file system built with the Raft consensus protocol
2. Prime Finder: A distributed prime number calculator that uses the AFS file system to store snapshots.

## Table of Contents

- [Test Cases](#test-case)
    - [Case 1](#1-basic-functionality)
    - [Case 2](#2-file-server-fault-tolerance)
    - [Case 3](#3-file-server-replication)
    - [Case 4](#4-distributed-prime-finder-ft)
- [Run Guide](#run-guide)
- [Integration](#integration)
- [File System](#afs-like-distributed-file-system-with-raft-consensus)
- [Prime Finder](#distributed-prime-finder)

---
## Test case

**Before test**
```bash
# nagivate to project file on VM
# '/Doanloads/DS_coursework'
cd Downloads
cd DS-2025-Group-coursework1
```
```bash
# clean cache
rm -rf AFS/srv_data/
rm -rf AFS/cache/
rm -rf srv_data/
rm -rf integration/cache/
rm -rf cache

# create files if not exist
mkdir -p tests/data/

# if use example input files
python -m upload
```

### 1. Basic Functionality

**Single Worker, Single File**
```bash
# create a file including 10 numbers
echo -e "2\n3\n4\n5\n6\n7\n8\n9\n11\n13" > tests/data/input_dataset_001.txt

# Terminal 1
python -m AFS.run_afs_server

# Terminal 2
python -m test_case_1

python -m integration.coordinator_afs \
  --input /tests/data/input_dataset_001.txt \
  --output /tests/outputs/result_001.txt \
  --workers 1

# Terminal 3
python -m integration.worker_afs 1
```

Check your output: `cat srv_data/tests/outputs/result_001.txt`
Should match:
```
2
3
5
7
11
13
```

**Multiple Workers, Multiple Files**
```bash
# File 1: Primes are 2, 3, 5
echo -e "2\n3\n4\n5\n6" > tests/data/input_dataset_001.txt

# File 2: Primes are 5, 7, 11 (Duplicate '5')
echo -e "5\n7\n8\n9\n11" > tests/data/input_dataset_002.txt

# File 3: Primes are 11, 13, 17 (Duplicate '11')
echo -e "11\n12\n13\n14\n17" > tests/data/input_dataset_003.txt

# Combine into 1 file
cat tests/data/input_dataset_001.txt \
    tests/data/input_dataset_002.txt \
    tests/data/input_dataset_003.txt \
    > tests/data/input_combined.txt
```
```bash
# Terminal 1
python -m AFS.run_afs_server

# Terminal 2
python -m test_case_2

python -m integration.coordinator_afs \
  --input /tests/data/input_combined.txt \
  --output /tests/outputs/result_merged.txt \
  --workers 3

# Terminal 3
python -m integration.worker_afs 1

# Terminal 4
python -m integration.worker_afs 2

# Terminal 5
python -m integration.worker_afs 3
```

Check your output: `cat srv_data/tests/outputs/result_merged.txt`
Should match:
```
2
3
5
7
11
13
17
```

### 2. File Server Fault Tolerance
**Server crash during read**
```bash
# create a file including 10000 numbers
python -m generate_10000

python -m test_case_3_1
```

**Server crash during read – Manual server control**
Workflow:
1. Start a single AFS server that will host the file.
2. Run the manual helper which uploads the payload.
3. Follow the prompts to kill the server at any time, then restart it.
4. The helper will read the file after restart and verify the data matches.

```bash
# Terminal 1
python -m AFS.run_afs_server 127.0.0.1 8888

# Terminal 2 (follow prompts to kill/restart server manually)
python -m tc31_manual_flow \
  --path /tests/data/tc31_manual_ctrl.txt \
  --address 127.0.0.1:8888 \
  --target-bytes 800000 


```
**Client crash during write**
```bash
python -m test_case_3_2
```

**Client crash during write – Manual client control**
Workflow:
1. Start a single AFS server on the desired port.
2. Run the manual helper; it prints the `tc32_clients` commands for partial and full writers.
3. Execute the partial client in another terminal and allow it to crash.
4. After validation, execute the healthy client and let the helper confirm the final result.

```bash
# Terminal 1
python -m AFS.run_afs_server 127.0.0.1 8888

# Terminal 2 (follow prompts; run the suggested client commands manually)
python -m tc32_manual_flow \
  --path /tests/data/tc32_manual_ctrl.txt \
  --host 127.0.0.1 \
  --port 8888
```

---
### 3. File Server Replication
**Replication & Recovery**
```bash
python -m test_case_4
```


**Replication & Recovery (manual Raft control)**
Workflow:
1. Start three `AFS.raft.server` instances (server1–server3) in separate terminals.
2. Run the manual flow helper to create and write the test file.
3. When prompted, manually kill the current leader’s terminal; after the write completes, restart that server.
4. The helper verifies that all replicas—including the restarted node—contain the latest payload.

```bash
# Terminal 1
python -m AFS.raft.server server1 127.0.0.1 8888 127.0.0.1:8889 127.0.0.1:8890

# Terminal 2
python -m AFS.raft.server server2 127.0.0.1 8889 127.0.0.1:8888 127.0.0.1:8890

# Terminal 3
python -m AFS.raft.server server3 127.0.0.1 8890 127.0.0.1:8888 127.0.0.1:8889

# Terminal 4 (follow prompts; kill/restart servers manually when asked)
python -m tc4_manual_flow \
  --path /tests/outputs/tc4_manual.txt \
  --addresses 127.0.0.1:8888 127.0.0.1:8889 127.0.0.1:8890
```

### 4. Distributed Prime Finder FT

**Coordinator Failure**
```bash
# Terminal 1
python -m AFS.run_afs_server

# Terminal 2
# create a file including 10000 numbers if not using example files
python -m generate_10000

# Note: remember to put the correct input file name you'd like to run
python -m integration.coordinator_afs \
  --input /tests/data/input_large.txt \
  --output /tests/outputs/result_large.txt \
  --workers 1

# Terminal 3
python -m integration.worker_afs 1
```
Check output from Terminal 2  
Wait til `[Snapshot] Snapshot 1 saved to AFS...`  
Then `Ctrl+C` manully kill the process  
Restart the terminal
```bash
# Terminal 2
# Note: remember to put the correct input file name you'd like to run
python -m integration.coordinator_afs \
  --input /tests/data/input_large.txt \
  --output /tests/outputs/result_large.txt
```

Check if this log is shown(numbers may differ):
```
Restored state: 1/4 tasks done. 24 primes found.
```

Then wail til exeucation successfully completed.

**Single Worker Failure**
```bash
# Terminal 1
python -m AFS.run_afs_server

# Terminal 2
# create a file including 10000 numbers if not using example files
python -m generate_10000

# Note: remember to put the correct input file name you'd like to run
python -m integration.coordinator_afs \
  --input /tests/data/input_large.txt \
  --output /tests/outputs/result_large.txt \
  --workers 1

# Terminal 3
python -m integration.worker_afs 1
```
After snapshots being saved, kill worker 1 by `ctrl+c`  
Expected output from Terminal 2: `Worker 1 (from ...) disconnected...`  
Restart worker 1: `python -m integration.worker_afs 1`  
You should see log in such format:
```
[Worker 1] Found snapshot state, recover from index 520
[Worker 1] Recovered from 520, rewinding to 420 for safety.
```

**Multiple Worker Failure**
```bash
# Terminal 1
python -m AFS.run_afs_server

# Terminal 2
# create a file including 10000 numbers if not using example files
python -m generate_10000

# start at least 2 works
# Note: remember to put the correct input file name you'd like to run
python -m integration.coordinator_afs \
  --input /tests/data/input_large.txt \
  --output /tests/outputs/result_large.txt \
  --workers 2

# Terminal 3
python -m integration.worker_afs 1

# Terminal 4
python -m integration.worker_afs 2
```
After snapshots being saved, kill worker 2 by `ctrl+c`  
Restart worker 2  
You should see log in such format:
```
[Worker 2] Found snapshot state, recover from index 520
[Worker 2] Recovered from 520, rewinding to 420 for safety.
```


## Run Guide
This guide explains run the integrated system.

**Step 1: Start AFS Cluster**

You have two options for running the file system:

Option 1: Single Server
```bash
# Terminal 1
python -m AFS.run_afs_server
```

Option 2: Raft Cluster
```bash
# Terminal 1: Server 1
python -m AFS.raft.server server1 127.0.0.1 8888 127.0.0.1:8889 127.0.0.1:8890

# Terminal 2: Server 2
python -m AFS.raft.server server2 127.0.0.1 8889 127.0.0.1:8888 127.0.0.1:8890

# Terminal 3: Server 3
python -m AFS.raft.server server3 127.0.0.1 8890 127.0.0.1:8888 127.0.0.1:8889
```

**Step 2: Generate and Upload Test Data**

```bash
# Generate test data (if not exists)
python -m tests.numers_generator

# Upload to AFS
python -m integration.data_to_afs
```

**Step 3: Start Coordinator**

```bash
# Terminal 4 (or Terminal 2 if using single server)
python -m integration.coordinator_afs
```

**Step 4: Start Workers**

```bash
# Terminal 5
python -m integration.worker_afs 1

# Terminal 6
python -m integration.worker_afs 2

# Terminal 7
python -m integration.worker_afs 3

# Terminal 8
python -m integration.worker_afs 4
```

---

## Integration

### Overview

The integration directory contains the final, combined system. It adapts the logic from tests to use the AFS as its storage backend.

1. **Coordinator**: Drives the Chandy-Lamport algorithm, sending marker messages and handling asyncio connections from workers
2. **Worker**: A stateless asyncio client that computes primes and responds to marker messages. It does not save its own state
3. **Snapshot Storage**: The Coordinator assembles the global snapshot and uses the AFS.client to persist it directly into the distributed file system, achieving fault-tolerant state storage

### Structure
```
integration/
├── coordinator_afs.py    # (Async) Coordinator, drives snapshots
├── data_to_afs.py        # (Async) Utility to upload data to AFS
├── snapshot_afs.py       # (Async) Snapshot logic, saves to AFS
└── worker_afs.py         # (Async) Stateless worker client
```

---

## AFS-Like Distributed File System with Raft Consensus

A fault-tolerant, replicated distributed file system inspired by the Andrew File System (AFS), implemented in Python using asyncio with Raft consensus for high availability.

### Overview

Implements a distributed file system with the following components:

1. **AFS-Like File System**: Client-server architecture with whole-file caching
2. **MiniRPC Framework**: Lightweight RPC system using asyncio and length-prefixed JSON protocol
3. **Fault Tolerance**: Comprehensive error handling, timeouts, retries, and idempotency
4. **Raft Consensus**: Leader election and log replication for high availability

### Structure
```
AFS/
├── afs/
│   ├── __init__.py
│   ├── client.py                   # AFS client with POSIX API + caching
│   └── handlers.py                 # Server handlers
├── cache/                          # Client-side cache
├── raft/                           # Raft consensus
│   ├── __init__.py
│   ├── rpc.py                      # Raft RPC communication
│   ├── server.py                   # Raft-enabled AFS server
│   └── state.py                    # Raft state machine (election, log)
├── rpc/
│   ├── __init__.py
│   ├── client.py                   # RPC client with failover
│   ├── framing.py                  # JSON framing protocol
│   └── server.py                   # Async RPC server
├── srv_data/                       # Server storage
│   ├── .meta.json                  # File metadata
│   └── demo.txt                    # Stored files
├── run_afs_server.py               # Single Server startup
└── ...
```
---

## Distributed Prime Finder

This is a distributed Coordinator-Worker system for parallel prime number computation, built entirely on `asyncio` to be compatible with the AFS client.

### Overview

This system implements a distributed prime finder based on the Chandy-Lamport global snapshot algorithm. The Coordinator actively manages state by sending marker messages, and Workers respond with their local state. The Coordinator then assembles a globally consistent snapshot, including "in-flight" messages, and persists this snapshot to the AFS.

### Structure
```
tests/
├── data/
│   └── test1000.txt        # Sample data
├── coordinator.py          # Original coordinator (threading-based)
├── numers_generator.py     # Generates the test data file
├── prime.py                # The core is_prime() function
├── readsnapshot.py         # Utility to read local snapshots
├── snapshot.py             # Original snapshot logic (threading-based)
└── worker.py               # Original worker (threading-based)
```
