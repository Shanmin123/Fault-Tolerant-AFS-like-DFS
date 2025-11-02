# test_afs_idempotency_client.py
# Purpose: Verify business idempotency (op_id) for side-effecting RPCs (e.g., Create).
# Expected: First call executes; repeated calls with the SAME op_id return the same
#           result immediately without reapplying side effects.

import asyncio
import sys

# Import depending on your project layout
try:
    from rpc.client import RPCClient, OK
except ImportError:  # fallback if your client is at project root as client.py
    from client import RPCClient, OK

# Allow overriding host/port from CLI: python this.py 127.0.0.1 8888
HOST = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
PORT = int(sys.argv[2]) if len(sys.argv) > 2 else 8888
HOST_PORT = f"{HOST}:{PORT}"

# ---- Idempotency test settings ----
IDEMP_OP = "Create"  # must be registered by your AFS server
IDEMP_ARGS = {
    # match your Create handler signature; keep only 'path'
    "path": "/tmp/afs_demo.txt",
}
IDEMP_OP_ID = "idemp-demo-0001"   # stable business id for this operation

async def main():
    client = RPCClient(
        servers=[HOST_PORT],
        connect_timeout=0.20,
        write_timeout=0.10,
        read_timeout=0.60,
        deadline_sec=1.20,   # harmless here; we’re not testing deadline in this demo
        retries=1,
        backoff_base=0.05,
    )

    print("== Step 1: Business idempotency (Create with a stable op_id) ==")
    r1 = await client.call(IDEMP_OP, IDEMP_ARGS, op_id=IDEMP_OP_ID)
    print("Create #1:", r1)

    print("\n== Step 2: Retry the SAME op_id (should hit idempotency and not re-apply) ==")
    r2 = await client.call(IDEMP_OP, IDEMP_ARGS, op_id=IDEMP_OP_ID)
    print("Create #2 (same op_id):", r2)

    same_code = (r1.get("code") == OK == r2.get("code"))
    same_data = (r1.get("data") == r2.get("data"))
    print("\nIdempotent behavior OK? ->", same_code and same_data)

if __name__ == "__main__":
    asyncio.run(main())