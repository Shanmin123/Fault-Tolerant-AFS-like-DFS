"""
Coordinator using AFS with Chandy-Lamport snapshot
"""
import pickle
import math
import asyncio
from typing import Set, List, Dict
from AFS.rpc.client import RPCClient
from AFS.afs.client import AFSClient
import primefinder.snapshot as snapshot

import os

HOST = 'localhost'
PORT = 5000
NUM_WORKERS = 4
SNAPSHOT_NAME = "coordinator_global_snapshot"

# AFS paths (instead of local file paths)
INPUT_FILE = "/AFS/srv_data/primefinder/data/test10000.txt"
OUTPUT_FILE = "/AFS/srv_data/primefinder/outputs/primesResult.txt"

class AFSCoordinator:
    def __init__(self, afs_servers):
        self.afs_servers = afs_servers
        self.afs: AFSClient = None
        self.primes: Set[int] = set()
        # {worker_id: writer}
        self.workers: Dict[int, asyncio.StreamWriter] = {}
        self.finished_workers: Set[int] = set()
        self.tasks: List[List[int]] = []
        self.next_worker_id = 1

    async def initialize_afs(self):
        """Initialize AFS client connection"""
        rpc = RPCClient(self.afs_servers, retries=2)
        self.afs = AFSClient(rpc=rpc)
        print(f"AFS client initialized with servers: {self.afs_servers}")

    async def read_from_afs(self, path: str):
        """Read numbers from AFS file"""
        try:
            fd = await self.afs.open(path, mode="r")
            content = self.afs.read(fd)
            await self.afs.close(fd)
            lines = content.decode('utf-8').split("\n")
            numbers = [int(line.strip()) for line in lines if line.strip()]
            print(f"Coordinator read {len(numbers)} numbers from AFS: {path}")
            return numbers
        except Exception as e:
            print(f"Error reading from AFS: {e}")
            raise

    async def save_to_afs(self, path: str, primes: set):
        """Save results to AFS"""
        try:
            result = "\n".join(str(p) for p in sorted(primes)) + "\n"
            try:
                fd = await self.afs.create(path)
            except:
                # File already exists, open for writing
                fd = await self.afs.open(path, mode="w")
            self.afs.write(fd, result.encode('utf-8'))
            await self.afs.close(fd)
            print(f"Coordinator saved {len(primes)} primes to AFS: {path}")
        except Exception as e:
            print(f"Error saving primes to AFS: {e}")

    def split_numbers(self, numbers, n_workers):
        """Split numbers into chunks for workers"""
        chunk_size = math.ceil(len(numbers) / n_workers)
        chunks = []
        i = 0
        while i < len(numbers):
            chunk = numbers[i:i + chunk_size]
            chunks.append(chunk)
            i += chunk_size
        return chunks
    
    async def snapshot_loop(self):
        """Periodic global snapshot using Chandy-Lamport algorithm"""
        while True:
            await asyncio.sleep(1)
            sid = snapshot.start(len(self.primes))
            if sid:
                print(f"[Coordinator] Starting snapshot {sid}")
                # Send marker messages to all workers
                marker = pickle.dumps({"type": "marker", "snapshot_id": sid})
                for worker_id, writer in self.workers.items():
                    try:
                        writer.write(marker)
                        await writer.drain()
                    except ConnectionError:
                        print(f"Error sending marker to worker {worker_id} (disconnected)")
    
    async def handle_worker(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """Handle individual worker connection"""
        addr = writer.get_extra_info('peername')
        print(f"Worker connected from {addr}")
        
        if not self.tasks:
            print("No tasks left for new worker")
            writer.close()
            await writer.wait_closed()
            return
            
        worker_id = self.next_worker_id
        self.next_worker_id += 1
        self.workers[worker_id] = writer
        task_chunk = self.tasks.pop(0)
        
        # Send task to worker
        task = pickle.dumps({"type": "task", "chunk": task_chunk, "workerid": worker_id})
        try:
            writer.write(task)
            await writer.drain()
            
            while True:
                data = await reader.read(4096)
                if not data:
                    raise ConnectionError("Worker disconnected")
                    
                message = pickle.loads(data)
                msg_type = message.get("type")

                if msg_type == "result":
                    self.primes.add(message["prime"])
                    snapshot.save_inflight(worker_id, message["prime"])
                    
                elif msg_type == "marker":
                    await snapshot.save_snapshot(
                        self.afs,
                        SNAPSHOT_NAME,
                        message["workerid"],
                        message["snapshot_id"],
                        message["state"]
                    )
                    
                elif msg_type == "reconnect":
                    print(f"Worker {worker_id} reconnecting, resending task")
                    task = pickle.dumps({"type": "task", "chunk": task_chunk, "workerid": worker_id})
                    writer.write(task)
                    await writer.drain()
                    
                elif msg_type == "finish":
                    print(f"Worker {worker_id} finished")
                    self.finished_workers.add(worker_id)
                    break

        except (ConnectionError, EOFError, ConnectionResetError) as e:
            print(f"Worker {worker_id} (from {addr}) disconnected: {e}")
        except Exception as e:
            print(f"Error with worker {worker_id}: {e}")
        finally:
            if worker_id in self.workers:
                del self.workers[worker_id]
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()

    async def run(self):
        """Main coordinator loop"""
        # Initialize AFS connection
        await self.initialize_afs()
        
        # Read input numbers from AFS
        numbers = await self.read_from_afs(INPUT_FILE)
        self.tasks = self.split_numbers(numbers, NUM_WORKERS)
        
        # Initialize snapshot system
        snapshot.init(NUM_WORKERS)
        
        # Start periodic snapshot task
        snapshot_task = asyncio.create_task(self.snapshot_loop())

        # Start server to accept worker connections
        server = await asyncio.start_server(self.handle_worker, HOST, PORT)
        addr = server.sockets[0].getsockname()
        print(f"Coordinator listening on {addr[0]}:{addr[1]}, waiting for {NUM_WORKERS} workers")

        # Wait for all workers to finish
        while len(self.finished_workers) < NUM_WORKERS:
            await asyncio.sleep(1)
            if not self.workers and not self.tasks and len(self.finished_workers) < NUM_WORKERS:
                print("All workers disconnected before finishing.")
                break
                
        print("All workers finished.")

        # Cleanup
        server.close()
        await server.wait_closed()
        snapshot_task.cancel()
        
        print(f"Total primes found: {len(self.primes)}")
        
        # Save results to AFS
        await self.save_to_afs(OUTPUT_FILE, self.primes)
        
        print("Coordinator completed successfully")


async def main():
    # Configure AFS servers
    # Single server mode:
    afs_servers = ["127.0.0.1:8888"]
    
    # Or Raft cluster mode (uncomment to use):
    # afs_servers = [
    #     "127.0.0.1:8888",
    #     "127.0.0.1:8889",
    #     "127.0.0.1:8890"
    # ]
    
    coordinator = AFSCoordinator(afs_servers)
    try:
        await coordinator.run()
    except KeyboardInterrupt:
        print("\nCoordinator interrupted by user.")
    except Exception as e:
        print(f"Coordinator error: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())
