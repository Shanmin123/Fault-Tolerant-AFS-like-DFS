"""
Raft RPC communication layer.
Implements Vote and Append RPCs for Raft consensus.
"""
import asyncio
from AFS.rpc.framing import write_frame, read_frame
from typing import List, Optional

class RaftRPC:
  def __init__(self, node_id: str):
    self.node_id = node_id

  async def send_vote_request(self, peer_ad: str, term: int, candidate_id: str, last_log_id: int, last_log_term: int) -> Optional[dict]:
    try:
      host, port = peer_ad.split(":")
      port = int(port)
      reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port),
        timeout=0.5
      )
      request = {
        "op": "VoteRequest",
        "req_od": "raft-vote",
        "args":{
          "term": term,
          "candidate_id": candidate_id,
          "last_log_id": last_log_id,
          "last_log_term": last_log_term
        }
      }
      await asyncio.wait_for(write_frame(writer, request), timeout=0.5)
      response = await asyncio.wait_for(read_frame(reader), timeout=0.5)
      writer.close()
      await writer.wait_closed()
      if isinstance(response, dict) and response.get("code") == 0:
        return response.get("data")
    except Exception as e:
      return None
  
  async def send_append_entries(self, peer_ad: str, term: int, leader_id: str, leader_commit: int, prev_log_id: int, prev_log_term: int, entries: List[dict]) -> Optional[dict]:
    try:
      host, port = peer_ad.split(":")
      port = int(port)
      reader, writer = await asyncio.wait_for(
        asyncio.open_connection(host, port),
        timeout=0.5
      )
      request = {
        "op": "AppendEntries",
        "req_id": "raft-append",
        "args":{
          "term": term,
          "leader_id": leader_id,
          "leader_commit": leader_commit,
          "prev_log_id": prev_log_id,
          "prev_log_term": prev_log_term,
          "entries": entries
        }
      }
      await asyncio.wait_for(write_frame(writer, request), timeout=0.5)
      response = await asyncio.wait_for(read_frame(reader), timeout=0.5)
      writer.close()
      await writer.wait_closed()
      if isinstance(response, dict) and response.get("code") == 0:
        return response.get("data")
    except Exception as e:
      return None
    
  async def broadcast_vote(self, peers: List[str], term: int, candidate_id: str, last_log_id: int, last_log_term: int) -> int:
    tasks = []
    for peer in peers:
      task = self.send_vote_request(peer, term, candidate_id, last_log_id, last_log_term)
      tasks.append(task)

    results = await asyncio.gather(*tasks, return_exceptions=True)
    #count votes
    votes = 1
    for result in results:
      if isinstance(result, dict) and result.get("is_vote"):
        votes += 1
    return votes
  
  async def send_hb(self, peers: List[str], term: int, leader_id: str, leader_commit: int, prev_log_id: int, prev_log_term: int):
    """send empty AppendEntries to all peers"""
    tasks = []
    for peer in peers:
      task = self.send_append_entries(peer_ad=peer, term=term, leader_id=leader_id, leader_commit=leader_commit, prev_log_id=prev_log_id, prev_log_term=prev_log_term, entries=[])
      tasks.append(task)
    await asyncio.gather(*tasks, return_exceptions=True)
  
  async def replicate_log(self, peer_ad: str, node) -> bool:
    next_id = node.next_id.get(peer_ad, 1)
    prev_log_id = next_id - 1
    prev_log_term = 0
    if prev_log_id > 0 and prev_log_id <= len(node.log):
      prev_log_term = node.log[prev_log_id - 1].term
    entries = []
    if next_id <= len(node.log):
      entries = [e.to_dict() for e in node.log[next_id - 1:]]
    
    response = await self.send_append_entries(
      peer_ad, node.current_term, node.node_id, node.commit_id, prev_log_id, prev_log_term, entries
    )
    if response is None:
      return False
    
    if response.get("term", 0) > node.current_term:
      node.follower(response["term"])
      return False
    if response.get("success"):
      if entries:
        node.match_id[peer_ad] = prev_log_id + len(entries)
        node.next_id[peer_ad] = node.match_id[peer_ad] + 1
      return True
    else:
      node.next_id[peer_ad] = max(1, next_id - 1)
      return False
