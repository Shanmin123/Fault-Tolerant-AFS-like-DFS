import argparse
import asyncio
import time
import uuid
from typing import Iterable, List

from tc4_rpc import FailoverRPC
from tc4_workflow import (
    bootstrap_file,
    make_payload,
    put_with_retry,
    wait_for_consistency,
)


class ManualCluster:
    """Simple adapter that exposes a static list of server addresses."""

    def __init__(self, addresses: Iterable[str]) -> None:
        self._addresses = list(addresses)

    def addresses(self, alive_only: bool = True) -> List[str]:
        return list(self._addresses)


async def manual_failover(
    path: str,
    addresses: List[str],
    base_lines: int,
    failover_lines: int,
) -> None:
    cluster = ManualCluster(addresses)
    rpc = FailoverRPC(cluster)
    base_payload = make_payload("replication-initial", base_lines)
    failover_payload = make_payload("replication-failover", failover_lines)

    version, leader = await bootstrap_file(rpc, path, base_payload)
    await wait_for_consistency(addresses, path, base_payload)
    print(f"[manual] baseline replicated via {leader} for {path}")

    print(
        f"[manual] starting large write via leader {leader}. "
        "Kill this server (close its terminal) while the write is running, "
        "then press Enter here once it is down."
    )
    write_task = asyncio.create_task(put_with_retry(rpc, path, failover_payload, version, prefer=leader))
    await asyncio.sleep(0.2)
    await asyncio.to_thread(input, ">>> Kill the leader now, then press Enter to continue...")
    new_version, new_leader = await write_task
    print(f"[manual] write finished with new leader {new_leader}, version {new_version}")

    alive_after = [addr for addr in addresses if addr != leader] or addresses
    await wait_for_consistency(alive_after, path, failover_payload)
    print("[manual] surviving replicas agree on failover payload")

    await asyncio.to_thread(
        input,
        f">>> Restart the crashed server ({leader}) and press Enter once it is back online...",
    )
    await wait_for_consistency(addresses, path, failover_payload)
    print("[manual] recovered replica caught up. Manual failover demo complete.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Manual failover workflow for test_case_4 with user-managed Raft servers."
    )
    parser.add_argument(
        "--path",
        default=f"/tests/outputs/tc4_manual_{int(time.time())}_{uuid.uuid4().hex}.txt",
        help="AFS file path used for the manual demonstration.",
    )
    parser.add_argument(
        "--addresses",
        nargs="+",
        default=["127.0.0.1:8888", "127.0.0.1:8889", "127.0.0.1:8890"],
        help="Addresses of the already-running Raft servers.",
    )
    parser.add_argument("--base-lines", type=int, default=64, help="Baseline payload size before failover.")
    parser.add_argument("--failover-lines", type=int, default=60000, help="Payload size used during failover write.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(manual_failover(args.path, args.addresses, args.base_lines, args.failover_lines))
    except KeyboardInterrupt:
        print("\nInterrupted by user")


if __name__ == "__main__":
    main()
