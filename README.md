# DS-2025-Group-coursework1

This is a distributed systems project composed of two main parts:
1. AFS/Raft: A distributed file system built with the Raft consensus protocol
2. Prime Finder: A distributed prime number calculator that uses the AFS file system to store snapshots.

## Table of Contents

- [Run Guide](#run-guide)
- [Integration](#integration)
- [File System](#afs-like-distributed-file-system-with-raft-consensus)
- [Prime Finder](#distributed-prime-finder)
- [Test Suite](#test-suite)

---

## Run Guide
This guide explains run the integrated system.

**Step 1: Start AFS Cluster**

You have two options for running the file system:

Option 1: Single Server
```bash
# Terminal 1
python -m DS_DFS_AFSlike.run_afs_server
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
python -m primefinder2_snapshot.numers_generator

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

The integration directory contains the final, combined system. It adapts the logic from primefinder to use the AFS as its storage backend.

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
primefinder/
├── data/
│   └── test1000.txt        # Sample data
├── coordinator.py          # Original coordinator (threading-based)
├── numers_generator.py     # Generates the test data file
├── prime.py                # The core is_prime() function
├── readsnapshot.py         # Utility to read local snapshots
├── snapshot.py             # Original snapshot logic (threading-based)
└── worker.py               # Original worker (threading-based)
```

---

## Test Suite

These tests are located in the AFS/ directory and are used to verify the file system's functionality independent of the Prime Finder.

`test_basic_operations.py`

Purpose: Verifies core file operations (create, write, read, close). It writes data, flushes it (on close), then re-opens and reads the data to ensure it matches.

How to Run:

```bash
# Against a single server (requires run_afs_server.py)
python -m AFS.test_basic_operations

# Against a full Raft cluster
python -m AFS.test_basic_operations --raft
```

`test_cache_validation.py`

Purpose: Tests the client-side whole-file caching. It demonstrates:

First open fetches from the server (Cache Miss).

Second open uses the local cache (Cache Hit).

A server-side modification invalidates the cache.

Third open re-fetches the new version (Cache Invalidation).

How to Run: (Requires run_afs_server.py)

```bash
python -m AFS.test_cache_validation
```

`test_afs_idempotency_client.py`

Purpose: Verifies that the server correctly handles idempotency keys (op_id). It sends the same Create command twice with the same op_id.

Expected Result: The first call executes, and the second call returns the exact same result without creating a second file or throwing an error.

How to Run: (Requires run_afs_server.py)

```bash
python -m AFS.test_afs_idempotency_client
```

`test_connect_cluster.py`

Purpose: A simple "smoke test" for the Raft cluster. It connects to all three servers, relies on the RPCClient to find the leader, and performs a single create/write/read cycle.

How to Run: (Requires the 3-node Raft cluster to be running)

```bash
python -m AFS.test_connect_cluster
```

`test_raft.py`

Purpose: A more comprehensive test suite for the Raft cluster. It checks cluster status, basic operations, read/write behavior on followers (writes should be rejected), and provides a prompt for manual leader failure testing.

How to Run: (Requires the 3-node Raft cluster to be running)

```bash
python -m AFS.test_raft
```
