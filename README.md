# DS-2025-Group-coursework1

## AFS-Like Distributed File System with Raft Consensus

A fault-tolerant, replicated distributed file system inspired by the Andrew File System (AFS), implemented in Python using asyncio with Raft consensus for high availability.

---

## Table of Contents

- [Overview](#overview)
- [Key Features](#key-features)
- [Project Structure](#project-structure)
- [Usage](#usage)
  - [Single Server Testing](#single-server-testing)
  - [Raft Cluster Testing](#raft-cluster-testing)
  - [Fault Tolerance Testing](#fault-tolerance-testing)
  - [Raft Testing](#raft-testing)
- [Fault Tolerance](#fault-tolerance)
- [Raft Configuration](#raft-consensus)

---

## Overview

This project implements a distributed file system with the following components:

1. **AFS-Like File System**: Client-server architecture with whole-file caching
2. **MiniRPC Framework**: Lightweight RPC system using asyncio and length-prefixed JSON protocol
3. **Fault Tolerance**: Comprehensive error handling, timeouts, retries, and idempotency
4. **Raft Consensus**: Leader election and log replication for high availability

---

## Key Features

### File System Features
- **Whole-file caching**: Download entire file on first open, subsequent operations are local
- **Cache validation**: TestAuth RPC checks if cached copy is still valid
- **Automatic flush**: Modified files uploaded to server on close
- **POSIX-like API**: Familiar `open()`, `read()`, `write()`, `close()` interface
- **Version tracking**: Optimistic concurrency control prevents lost updates

### Fault Tolerance Features
- **Connection failures**: Automatic retry with exponential backoff (0.2s, 0.4s, 0.8s...)
- **Server crashes**: Client switches to alternative servers via round-robin
- **Lost messages**: Timeout-based retransmission for network failures
- **Duplicate requests**: Operation IDs prevent double-execution (at-most-once semantics)
- **Orphan detection**: End-to-end deadlines prevent zombie computations
- **Graceful degradation**: Specific error codes (connect failed, write failed, read failed)

### Replication Features
- **Leader election**: Automatic with randomized timeouts (150-300ms)
- **Log replication**: All writes replicated to majority before commit
- **Strong consistency**: Linearizable reads and writes through leader
- **Crash recovery**: Persistent Raft state survives restarts
- **Split-brain prevention**: Majority quorum required for all operations

---

## Project structure
```
DFS/
├── afs/
│   ├── __init__.py
│   ├── client.py                   # AFS client with POSIX API + caching
│   └── handlers.py                 # Server handlers
├── cache/                          # Client-side cache
│   ├── .meta.json                  # Version tracking
│   └── demo.txt                    # Cached files
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
├── srv_data/                       # Server storage (single server)
│   ├── .meta.json                  # File metadata
│   └── demo.txt                    # Stored files
├── afs_client_demo.py              # High-level API demo
├── afs_smoke.py                    # Low-level RPC test
├── api_demo.py                     # POSIX API demo
├── oplog.db                        # Idempotency log (SQLite)
├── run_afs_server.py               # Single Server startup
├── test_afs_idempotency_client.py
├── test_basic_operations.py
├── test_cache_validation.py
├── test_connect_cluster.py
└── test_raft.py
```

---

## Setup

### 1. Create Required Directories

```bash
# Create package marker files
touch rpc/__init__.py
touch afs/__init__.py
touch raft/__init__.py

# Create data directories (auto-created if missing)
mkdir -p srv_data
mkdir -p cache
```

### 2. Prepare Test Data

```bash
# Create a test file for the server
echo "Initial file content" > srv_data/demo.txt
```

---

## Usage

### Test Files Overview

| Test File | Purpose | Requires |
|-----------|---------|----------|
| `test_basic_operations.py` | Create, write, read, close operations | Single server or Raft |
| `test_cache_validation.py` | Cache hit/miss with TestAuth | Single server |
| `test_connect_cluster.py` | Simple Raft connection test | 3 Raft servers |
| `test_raft.py` | Full Raft test suite | 3 Raft servers |
| `afs_smoke.py` | Low-level RPC smoke test | Single server |
| `api_demo.py` | POSIX API demo | Single server |
| `test_afs_idempotency_client.py` | Idempotency testing | Single server |


### Single Server Testing

#### Setup
```bash
# Default (127.0.0.1:8888)
python run_afs_server.py

# Custom host/port
python run_afs_server.py 0.0.0.0 9999
```

#### Run Tests
```bash
# Basic operations
python test_basic_operations.py

# Cache validation
python test_cache_validation.py

# Low-level RPC test
python afs_smoke.py

# High-level API test
python afs_client_demo.py

# POSIX-like API test
python api_demo.py
```

### Raft Cluster Testing

#### Setup (3 Terminals)
```bash
# Terminal 1: Server 1
python -m raft.server server1 127.0.0.1 8888 127.0.0.1:8889 127.0.0.1:8890

# Terminal 2: Server 2
python -m raft.server server2 127.0.0.1 8889 127.0.0.1:8888 127.0.0.1:8890

# Terminal 3: Server 3
python -m raft.server server3 127.0.0.1 8890 127.0.0.1:8888 127.0.0.1:8889
```

**Wait for Leader Election:** `[Raft-serverX] Became leader for term N`

#### Run Tests (Terminal 4)
```bash
# Simple connection test
python test_connect_cluster.py

# Basic operations through Raft
python test_basic_operations.py --raft

# Full Raft test suite
python test_raft.py
```

### Fault Tolerance Testing

**Test 1: Server Crash**
```bash
# Start server
python run_afs_server.py

# In another terminal, run client
python api_demo.py

# Kill server (Ctrl+C)
# Client should get error: {"code": 1002, "err": "CONNECTION_REFUSED"}
```

**Test 2: Network Timeout**
```bash
# Client with short timeout
python -c "
import asyncio
from rpc.client import RPCClient

async def test():
    rpc = RPCClient(['127.0.0.1:9999'], read_timeout=0.5, retries=2)
    r = await rpc.call('Open', {'path': '/file.txt'})
    print('Result:', r)

asyncio.run(test())
"
# Output: {"code": 1002, "err": "CONNECTION_REFUSED"}
```

**Test 3: Idempotency**
```bash
python test_afs_idempotency_client.py
# Should show same result for repeated calls with same op_id
```

### Raft Testing

**Test 1: Leader Failure**
```bash
# 1. Start 3-server cluster
# 2. Identify leader from logs
# 3. Kill leader process
# 4. Wait 150-300ms for new election
# 5. Run client operations - should still work!
```

**Test 2: Split Brain**
```bash
# 1. Start 3-server cluster
# 2. Disconnect server3 from network
# 3. Servers 1-2 still have majority → continue operating
# 4. Server 3 cannot become leader (no majority)
```

**Test 3: Crash Recovery**
```bash
# 1. Start cluster, create files
# 2. Kill one server
# 3. Restart server
# 4. Server replays Raft log, catches up
# 5. Files are consistent across all servers
```

---

## Fault Tolerance

### Error Handling Strategy

| Scenario | Detection | Recovery | Client Impact |
|----------|-----------|----------|---------------|
| **Server Down** | Connection refused | Try next server | Transparent failover |
| **Network Partition** | Connection timeout | Round-robin retry | Automatic retry |
| **Lost Request** | Write timeout (0.1s) | Retransmit | Transparent retry |
| **Lost Response** | Read timeout (0.6s) | Retransmit | Idempotent replay |
| **Server Crash** | No response | Failover to replica | Leader re-election |
| **Duplicate Request** | op_id check | Return cached result | At-most-once |
| **Client Crash** | Deadline exceeded | Cancel operation | Orphan prevention |

### Timeout Configuration

```python
RPCClient(
    servers=["127.0.0.1:8888", "127.0.0.1:8889"],
    connect_timeout=0.20,   # Connection establishment
    write_timeout=0.10,     # Sending request
    read_timeout=0.60,      # Receiving response
    deadline_sec=1.80,      # End-to-end deadline
    retries=1,              # Extra attempts after first try
    backoff_base=0.2        # Exponential backoff base
)
```

### Error Codes

| Code | Name | Meaning | Client Action |
|------|------|---------|---------------|
| 0 | OK | Success | Continue |
| 1001 | E_TIMEOUT | Generic timeout | Retry |
| 1002 | E_CONNECT_FAILED | Cannot connect | Try next server |
| 1003 | E_WRITE_FAILED | Send failed | Retry |
| 1004 | E_READ_FAILED | Receive failed | Retry |
| 1999 | E_INTERNAL | Server error | Report to user |

---

## Raft Consensus

### Raft Configuration

**Election Timeout:** 150-300ms (randomized)
```python
def _random_election_timeout(self) -> float:
    return random.uniform(0.15, 0.30)
```

**Heartbeat Interval:** 50ms
```python
await asyncio.sleep(0.05)  # Send heartbeats every 50ms
```

**Commit Timeout:** 5 seconds
```python
timeout = 5.0  # Wait up to 5s for commit
```