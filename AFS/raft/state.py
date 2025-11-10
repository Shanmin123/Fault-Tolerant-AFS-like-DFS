import asyncio
import random
import time
import json
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from enum import Enum

class Node(Enum):
  LEADER = "leader"
  CANDIDATE = "candidate"
  FOLLOWER = "follower"

class LogEntry:
  def __init__(self, term: int, index: int, command: dict):
    self.term = term
    self.index = index
    self.command = command
  def to_dict(self) -> dict:
    return{
      "term": self.term,
      "index": self.index,
      "command": self.command
    }
  def from_dict(data: dict) -> "LogEntry":
    return LogEntry(data["term"], data["index"], data["command"])
  
class RaftNode:
  """leader election + log replication"""
  def __init__(self, node_id: str, peer_adr: List[str], storage_dir: str):
    self.node_id = node_id
    self.peer_adr = peer_adr
    self.storage_dir = Path(storage_dir)
    self.storage_dir.mkdir(parents=True, exist_ok=True)
    #persistent
    self.current_term = 0
    self.voted_for: Optional[str] = None
    self.log: List[LogEntry] = []
    #all servers
    self.commit_id = 0
    self.last_applied = 0
    self.state = Node.FOLLOWER
    #leaders only
    self.next_id: Dict[str, int] = {}
    self.match_id: Dict[str, int] = {}
    #time
    self.last_heartbeat = time.time()
    self.election_timeout = self._random_election_timeout()
    #callback
    self.commit_callback = None
    self._load_state()
  
  def _random_election_timeout(self) -> float:
    return random.uniform(0.10, 0.30)
  
  def _save_state(self):
    state = {
      "current_term": self.current_term,
      "voted_for": self.voted_for,
      "log": [entry.to_dict() for entry in self.log]
    }
    
    state_file = self.storage_dir / "raft_state.json"
    temp_file = state_file.with_suffix(".tmp")
    
    temp_file.write_text(json.dumps(state, indent=2))
    temp_file.replace(state_file)

  def _load_state(self):
    state_file = self.storage_dir / "raft_state.json"
    if state_file.exists():
      state = json.loads(state_file.read_text())
      self.current_term = state["current_term"]
      self.voted_for = state.get("voted_for")
      self.log = [LogEntry.from_dict(e) for e in state["log"]]

  def append_entry(self, command: dict) -> int:
    if self.state != Node.LEADER:
      raise Exception("Only leaders can append entries")
    index = len(self.log) + 1
    entry = LogEntry(self.current_term, index, command)
    self.log.append(entry)
    self._save_state()
    return index
  
  def get_last_log(self) -> Tuple[int, int]:
    if not self.log:
      return(0, 0)
    last = self.log[-1]
    return (last.index, last.term)
  
  def is_up_to_date(self, last_log_index: int, last_log_term: int) -> bool:
    our_last_index, our_last_term = self.get_last_log()
    if last_log_term != our_last_term:
      return last_log_term > our_last_term
    return last_log_index >= our_last_index
  
  def election(self):
    self.state = Node.CANDIDATE
    self.current_term += 1
    self.voted_for = self.node_id
    self.election_timeout = self._random_election_timeout()
    self.last_heartbeat = time.time()
    self._save_state()
    print(f"[Raft-{self.node_id}] Starting election for term {self.current_term}")

  def leader(self):
    self.state = Node.LEADER
    last_log_id = len(self.log)
    for peer in self.peer_adr:
      self.next_id[peer] = last_log_id + 1
      self.match_id[peer] = 0
    print(f"[Raft-{self.node_id}] Became leader for term {self.current_term}")

  def follower(self, term: int):
    self.state = Node.FOLLOWER
    self.current_term = term
    self.voted_for = None
    self._save_state()
    print(f"[Raft-{self.node_id}] Became follower for term {self.current_term}")
  
  def reset_election_time(self):
    self.last_heartbeat = time.time()
    self.election_timeout = self._random_election_timeout()

  def start_election(self) -> bool:
    if self.state == Node.LEADER:
      return False
    passed = time.time() - self.last_heartbeat
    return passed > self.election_timeout
  
  def update_commit_id(self):
    if self.state != Node.LEADER:
      return
    servers = len(self.peer_adr) + 1
    majority = (servers // 2) + 1
    for n in range(len(self.log), self.commit_id, -1):
      if n == 0:
        break
      replicated = 1
      for peer in self.peer_adr:
        if self.match_id.get(peer, 0) >= n:
          replicated += 1
      if replicated >= majority and self.log[n-1].term == self.current_term:
        self.commit_id = n
        print(f"[Raft-{self.node_id}] Updated commit_index to {n}")
        break

  def apply_commited_entries(self):
    while self.last_applied < self.commit_id:
      self.last_applied += 1
      entry = self.log[self.last_applied - 1]
      if self.commit_callback:
        self.commit_callback(entry)
  
  def append_entries(self, term: int, leader_id: str, prev_log_id: int, prev_log_term: int, entries: list[dict], leader_commit: int) -> Tuple[bool, int]:
    if term < self.current_term:
      return (False, self.current_term)
    if term > self.current_term:
      self.follower(term)
    self.reset_election_time()
    #false if log doesn't contain entry at previous index with term
    if prev_log_id > 0:
      if prev_log_id > len(self.log):
        return (False, self.current_term)
      
      if self.log[prev_log_id - 1].term != prev_log_term:
        self.log = self.log[:prev_log_id - 1]
        self._save_state()
        return (False, self.current_term)
    
    #append new
    for i, entry_dict in enumerate(entries):
      entry = LogEntry.from_dict(entry_dict)
      log_id = prev_log_id + i + 1
      if log_id > len(self.log):
        self.log.append(entry)
      elif self.log[log_id - 1].term != entry.term:
        self.log = self.log[:log_id - 1]
        self.log.append(entry)
    self._save_state()

    if leader_commit > self.commit_id:
      self.commit_id = min(leader_commit, len(self.log))

    return (True, self.current_term)
  
  def vote(self, term: int, candidate_id: str, last_log_id: int, last_log_term: int) -> Tuple[bool, int]:
    if term < self.current_term:
      return (False, self.current_term)
    if term > self.current_term:
      self.follower(term)
    
    can_vote = (self.voted_for is None or self.voted_for == candidate_id)
    log_ok = self.is_up_to_date(last_log_id, last_log_term)
    if can_vote and log_ok:
      self.voted_for = candidate_id
      self.reset_election_time()
      self._save_state()
      return (True, self.current_term)
    
    return (False, self.current_term)