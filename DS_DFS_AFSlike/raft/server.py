"""
Raft-enabled AFS Server.
Integrates Raft consensus with the AFS RPC server for replicated file storage.
"""
import asyncio
import sys
from typing import Optional, List, Dict
from afs.handlers import Open, TestAuth, Create, GetFile, PutFile
from rpc.server import RPCServer
from rpc.framing import read_frame, write_frame
from raft.state import Node, RaftNode
from raft.rpc import RaftRPC

class RaftServer:
  """
  Client RPCs handled by leader, Raft replicated
  Raft RPCs handled by nodes
  """
  def __init__(self, host: str, port: int, node_id: str, peers: List[str], storage_dir: str):
    self.host = host
    self.port = port
    self.node_id = node_id
    self.storage_dir = storage_dir
    self.raft = RaftNode(node_id, peer_adr=peers, storage_dir=f"{storage_dir}/raft")
    self.raft_rpc = RaftRPC(node_id)
    self.rpc_server = RPCServer()
    #track last known leader
    self.last_leader: Optional[str] = None
    self.peer_adr: Dict[str, str] = {}
    for i, peer in enumerate(peers):
      peer_id = f"server{i+2}" if node_id == "server1" else f"server{i+1}"
      self.peer_adr[peer_id] = peer
    self.peer_adr[node_id] = f"{host}:{port}"

    #register handlers
    self._handlers()
    self.rpc_server.register("VoteRequest", self.handle_request_vote)
    self.rpc_server.register("AppendEntries", self.handle_append_entries)
    
    self.raft.commit_callback = self.apply_to_state_machine

    self.running = False
    self.election_task: Optional[asyncio.Task] = None
    self.heartbeat_task: Optional[asyncio.Task] = None
  
  def _handlers(self):
    self.rpc_server.register("Open", Open)
    self.rpc_server.register("TestAuth", TestAuth)
    self.rpc_server.register("GetFile", GetFile)
    #go through Raft
    self.rpc_server.register("Create", self.handle_create)
    self.rpc_server.register("PutFile", self.handle_put_file)

  async def handle_request_vote(self, term: int, candidate_id: str, last_log_id: int, last_log_term: int) -> dict:
    vote, current_term = self.raft.vote(term, candidate_id, last_log_id, last_log_term)
    return {"is_vote": vote, "term": current_term}

  async def handle_append_entries(self, term: int, leader_id: str, prev_log_id: int, prev_log_term: int, entries: list, leader_commit: int) -> dict:
    #track the leader for _get_last_leader()
    if term >= self.raft.current_term:
      self.last_leader = leader_id

    success, current_term = self.raft.append_entries(term, leader_id, prev_log_id, prev_log_term, entries, leader_commit)
    self.raft.apply_commited_entries()
    return {"success": success, "term": current_term}
    
  def apply_to_state_machine(self, entry):
    """
    Apply a commited log entry to AFS state machine
    Use op_id idempotency to ensure operations are only applied once even called multiple times    
    """
    command = entry.command
    op = command["op"]
    args = command["args"]
    print(f"[RaftAFS-{self.node_id}] Applying {op} at index {entry.index}")
    #idempotency
    op_id = f"raft-{entry.index}"
    try:
      if op == "Create":
        asyncio.create_task(self.apply_create_idem(args, op_id))
      elif op == "PutFile":
        asyncio.create_task(self.apply_put_idem(args, op_id))
      else:
        print(f"[RaftAFS-{self.node_id}] Unknown operation: {op}")
    except Exception as e:
      print(f"[RaftAFS-{self.node_id}] Error applying {op}: {e}")
  async def apply_create_idem(self, args: dict, op_id: str):
    try:
      result = await Create(**args)
      print(f"[RaftAFS-{self.node_id}] Applied Create: {args['path']}")
    except FileExistsError:
      print(f"[RaftAFS-{self.node_id}] Create idempotent (already exists): {args['path']}")
    except Exception as e:
      print(f"[RaftAFS-{self.node_id}] Failed to apply Create: {e}")
  async def apply_put_idem(self, args: dict, op_id: str):
    try:
      result = await PutFile(**args)
      print(f"[RaftAFS-{self.node_id}] Applied PutFile: {args['path']}")
    except ValueError as e:
      if "version conflict" in str(e):
        #alreafy applied
        print(f"[RaftAFS-{self.node_id}] PutFile idempotent (version conflict): {args['path']}")
      else:
        raise
    except Exception as e:
      print(f"[RaftAFS-{self.node_id}] Failed to apply PutFile: {e}")

  async def handle_create(self, path: str, **kwargs) -> dict:
    if self.raft.state != Node.LEADER:
      return  {"error": "not_leader", "leader_hint": self._get_last_leader()}
    command = {"op": "Create", "args": {"path": path, **kwargs}}
    log_id = self.raft.append_entry(command)

    timeout = 5.0
    start = asyncio.get_event_loop().time()
    while self.raft.commit_id < log_id:
      if asyncio.get_event_loop().time() - start > timeout:
        return {"error": "commit_timeout"}
      await asyncio.sleep(0.01)
    entry = self.raft.log[log_id - 1]
    return await self.apply_create(entry.command["args"])
  
  async def apply_create(self, args: dict) -> dict:
    return await Create(**args)

  async def handle_put_file(self, path: str, bytes: str, base_version: int, **kwargs) -> dict:
    if self.raft.state != Node.LEADER:
      return {"error": "not_leader", "leader_hint": self._get_last_leader()}
    command = {"op": "PutFile", "args": {"path": path, "bytes": bytes, "base_version": base_version, **kwargs}}
    log_id = self.raft.append_entry(command)

    timeout = 5.0
    start = asyncio.get_event_loop().time()
    while self.raft.commit_id < log_id:
      if asyncio.get_event_loop().time() - start > timeout:
        return {"error": "commit_timeout"}
      await asyncio.sleep(0.01)
    entry = self.raft.log[log_id - 1]
    return await self.apply_put_file(entry.command["args"])

  async def apply_put_file(self, args: dict) -> dict:
    return await PutFile(**args)

  async def _election_loop(self):
    while self.running:
      await asyncio.sleep(0.05)
      if self.raft.start_election():
        self.raft.election()
        last_log_id, last_log_term = self.raft.get_last_log()
        votes = await self.raft_rpc.broadcast_vote(
          self.raft.peer_adr, self.raft.current_term, self.raft.node_id, last_log_id, last_log_term
        )
        servers = len(self.raft.peer_adr) + 1
        majority = (servers // 2)+1
        if votes >= majority and self.raft.state == Node.CANDIDATE:
          self.raft.leader()

  async def _heartbeat_loop(self):
    while self.running:
      await asyncio.sleep(0.05)
      if self.raft.state == Node.LEADER:
        last_log_id, last_log_term = self.raft.get_last_log()
        await self.raft_rpc.send_hb(
          self.raft.peer_adr, self.raft.current_term, self.raft.node_id, self.raft.commit_id, last_log_id, last_log_term
        )
        for peer in self.raft.peer_adr:
          await self.raft_rpc.replicate_log(peer, self.raft)
        self.raft.update_commit_id()
        self.raft.apply_commited_entries()

  async def serve(self):
    """start the server"""
    self.running = True
    self.election_task = asyncio.create_task(self._election_loop())
    self.heartbeat_task = asyncio.create_task(self._heartbeat_loop())
    print(f"[RaftAFS-{self.node_id}] Starting server on {self.host}:{self.port}")
    print(f"[RaftAFS-{self.node_id}] Peers: {self.raft.peers}")
    #start RPC server
    await self.rpc_server.serve(self.host, self.port)

  async def shutdown(self):
    print(f"[RaftAFS-{self.node_id}] Shutting down...")
    self.running = False
    if self.election_task: self.election_task.cancel()
    if self.heartbeat_task: self.heartbeat_task.cancel()

  def _get_last_leader(self) -> Optional[str]:
    """
    clients find the leader by reaching to the address of last known leader, rather than try all servers
    Return: leader address or None
    """
    if self.raft.state == Node.LEADER:
      return f"{self.host}:{self.port}"
    if self.last_leader:
      return self.peer_adr.get(self.last_leader)
    return None

if __name__ == "__main__":
  if len(sys.argv) < 4:
    print("Usage: python -m raft.server <node_id> <host> <port> <peer1:port> <peer2:port> ...")
    print("Example: python -m raft.server server1 127.0.0.1 8888 127.0.0.1:8889 127.0.0.1:8890")
    sys.exit(1)
  
  node_id = sys.argv[1]
  host = sys.argv[2]
  port = int(sys.argv[3])
  peers = sys.argv[4:] if len(sys.argv) > 4 else []
  storage_dir = f"./storage_{node_id}"
  server = RaftServer(host=host, port=port, node_id=node_id, peers=peers, storage_dir=storage_dir)
  try:
    asyncio.run(server.serve())
  except KeyboardInterrupt:
    print(f"\n[RaftAFS-{node_id}] Interrupted")