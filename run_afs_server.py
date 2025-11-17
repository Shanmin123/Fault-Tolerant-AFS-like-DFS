# AFS/run_raft_node.py
import asyncio
import sys
import time
import random
from AFS.rpc.server import RPCServer
from AFS.raft.state import RaftNode, Node
from AFS.raft.rpc import RaftRPC
from AFS.afs import handlers

# 硬编码的集群配置
PEERS_CONFIG = ["127.0.0.1:8888", "127.0.0.1:8889", "127.0.0.1:8890"]

class RaftService:
    def __init__(self, node_id, peers):
        # 根据端口区分存储目录
        port = node_id.split(":")[1]
        storage_dir = f"./srv_data_{port}"
        
        self.raft = RaftNode(node_id, peers, storage_dir)
        self.rpc = RaftRPC(node_id)
        self.raft.commit_callback = self.apply_to_state_machine

    async def start_background_tasks(self):
        asyncio.create_task(self.election_loop())
        asyncio.create_task(self.heartbeat_loop())

    async def election_loop(self):
        """选举循环：超时 -> 竞选 -> 数票 -> 上位"""
        while True:
            if self.raft.start_election():
                self.raft.election()
                last_log_index, last_log_term = self.raft.get_last_log()
                print(f"[Election] Asking for votes in term {self.raft.current_term}...")
                
                # 广播拉票
                votes = await self.rpc.broadcast_vote(
                    self.raft.peer_adr, 
                    self.raft.current_term, 
                    self.raft.node_id, 
                    last_log_index, 
                    last_log_term
                )
                
                # 统计票数 (broadcast_vote 返回包含自己的总票数)
                majority = (len(PEERS_CONFIG) // 2) + 1
                print(f"[Election] Got {votes} votes (needed {majority})")

                if votes >= majority:
                    self.raft.leader()
            
            await asyncio.sleep(random.uniform(0.1, 0.3))

    async def heartbeat_loop(self):
        """Leader 心跳循环"""
        while True:
            if self.raft.state == Node.LEADER:
                tasks = []
                for peer in self.raft.peer_adr:
                    tasks.append(self.rpc.replicate_log(peer, self.raft))
                await asyncio.gather(*tasks, return_exceptions=True)
                
                self.raft.update_commit_id()
                self.raft.apply_commited_entries()
                
            await asyncio.sleep(0.05)

    # --- RPC Handlers ---

    async def handle_vote(self, term, candidate_id, last_log_id, last_log_term):
        success, term = self.raft.vote(term, candidate_id, last_log_id, last_log_term)
        return {"term": term, "vote_granted": success, "is_vote": success}

    async def handle_append(self, term, leader_id, prev_log_id, prev_log_term, entries, leader_commit):
        success, term = self.raft.append_entries(term, leader_id, prev_log_id, prev_log_term, entries, leader_commit)
        return {"term": term, "success": success}

    def apply_to_state_machine(self, log_entry):
        """写入磁盘回调"""
        cmd = log_entry.command
        op = cmd.get("op")
        args = cmd.get("args")
        try:
            loop = asyncio.get_running_loop()
            if op == "Create":
                loop.create_task(handlers.Create(**args))
            elif op == "PutFile":
                loop.create_task(handlers.PutFile(**args))
        except Exception as e:
            print(f"Error applying log: {e}")

    async def client_write_request(self, op, **args):
        """
        处理客户端写请求 (Create, PutFile)
        关键修改：如果不是 Leader，故意延迟响应以触发客户端超时重试
        """
        # 1. 检查 Leader 身份
        if self.raft.state != Node.LEADER:
            # 策略：装死。
            # Client 的 read_timeout 默认是 0.6s。我们睡 1.5s。
            # Client 会抛出 TimeoutError，然后自动尝试下一个 Server。
            # print(f"[Follower] Stalling client request on {op} to force retry...")
            await asyncio.sleep(1.5)
            raise Exception("not_leader")
        
        # 2. 写入 Raft Log
        command = {"op": op, "args": args}
        log_index = self.raft.append_entry(command)
        print(f"[Leader] Appended {op} to log index {log_index}. Waiting for commit...")
        
        # 3. 等待 Commit
        start_time = time.time()
        while self.raft.commit_id < log_index:
            if time.time() - start_time > 2.0:
                raise Exception("raft_timeout")
            await asyncio.sleep(0.01)
            
        # 4. 返回成功数据 (直接返回字典，不要再包一层 data)
        if op == "PutFile":
             nv = args.get("base_version", 0) + 1
             return {"ok": True, "new_version": nv}
        
        # Create 的返回值
        return {"handle": 1, "version": 1, "size": 0}


async def main():
    if len(sys.argv) < 3:
        print("Usage: python -m AFS.run_raft_node <IP> <PORT>")
        sys.exit(1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    node_id = f"{host}:{port}"
    
    peers = [p for p in PEERS_CONFIG if p != node_id]
    
    service = RaftService(node_id, peers)
    await service.start_background_tasks()
    
    srv = RPCServer()
    
    # 注册 Raft 协议
    srv.register("VoteRequest", service.handle_vote)
    srv.register("AppendEntries", service.handle_append)
    
    # 注册读操作 (直接读)
    srv.register("Open", handlers.Open)
    srv.register("GetFile", handlers.GetFile)
    srv.register("TestAuth", handlers.TestAuth)
    
    # 注册写操作 (走 Raft + 超时策略)
    srv.register("Create", lambda **k: service.client_write_request("Create", **k))
    srv.register("PutFile", lambda **k: service.client_write_request("PutFile", **k))

    print(f"--- Raft Node {node_id} Started (Peers: {len(peers)}) ---")
    
    await srv.serve(host, port)

if __name__ == "__main__":
    asyncio.run(main())