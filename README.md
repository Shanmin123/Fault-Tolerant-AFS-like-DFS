# DS-2025-Group-coursework1
## MiniRPC

A minimal RPC framework implemented in Python using asyncio and a length-prefixed JSON protocol.
### Overview

**MiniRPC** is a lightweight RPC system that lets clients call server functions over TCP as if they were local.
Each request is a JSON message framed by a 4-byte big-endian length prefix.

This project demonstrates:
- Safe message framing (no sticky/half packet problem)
- Async server and client built on `asyncio`
- Timeout and retry handling on the client side
- Clear separation between transport (TCP) and logic (RPC)

### Project structure
```
./
├── afs/
│   ├── __init__.py
│   ├── client.py            # AFS client with POSIX API
│   └── handlers.py          # Server handlers
├── cache/
│   ├── .meta.json
│   ├── demo.txt
├── rpc/
│   ├── __init__.py
│   ├── client_rpc.py        # RPC client with failover
│   ├── framing.py           # JSON framing protocol
│   └── server.py            # Async RPC server
├── srv_data/
│   ├── .meta.json
│   ├── demo.txt
├── afs_client_demo.py       # High-level API demo
├── afs_smoke.py             # RPC smoke test
├── api_demo.py              # POSIX API demo
└── run_afs_server.py        # Server startup
```

## Setup

### 1. Create Required Directories

```bash
# Create package marker files
touch rpc/__init__.py
touch afs/__init__.py
```

### 2. Prepare Test Data

```bash
# Create a test file for the server
mkdir -p srv_data
echo "Initial file content" > srv_data/demo.txt
```

## Usage

### Start the Server

```bash
python run_afs_server.py

# Custom host/port
python run_afs_server.py 0.0.0.0 9999
```

### Run Demos

**RPC test:**
```bash
python afs_smoke.py
```

**API test:**
```bash
python afs_client_demo.py
```

**POSIX-like API test:**
```bash
python api_demo.py
```

## Cache testing
```bash
# Terminal 1: Start server
python run_afs_server.py

# Terminal 2: First client - creates file
python -c "
import asyncio
from rpc.client import RPCClient
from afs.client import AFSClient

async def test():
    afs = AFSClient(RPCClient(['127.0.0.1:8888']))
    fd = await afs.create('/test.txt')
    afs.write(fd, b'Version 1\n')
    await afs.close(fd)
    print('Created file')

asyncio.run(test())
"

# Terminal 3: Second client - uses cached copy
python -c "
import asyncio
from rpc.client import RPCClient
from afs.client import AFSClient

async def test():
    afs = AFSClient(RPCClient(['127.0.0.1:8888']))
    
    # First open - fetches from server
    fd = await afs.open('/test.txt', 'r')
    print('First:', afs.read(fd))
    await afs.close(fd)
    
    # Second open - uses cache (TestAuth returns valid)
    fd = await afs.open('/test.txt', 'r')
    print('Second (cached):', afs.read(fd))
    await afs.close(fd)

asyncio.run(test())
"
```