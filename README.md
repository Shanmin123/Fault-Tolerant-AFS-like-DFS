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
