import socket
import pickle
import math
import send_data, recv_data
import time
import threading


HOST = 'localhost'
PORT = 5000
NUM_workers = 4


def read_numbers(file_path):
    with open(file_path, 'r') as f:
        return [int(line.strip()) for line in f if line.strip()]

def split_numbers(num, n_workers):
    chunk_size = math.ceil(len(num) / n_workers)
    return [num[i:i+chunk_size] for i in range(0, len(num), chunk_size)]

def coordinator_main():
    #read data and separate as chunks
    numbers = read_numbers("data/test1.txt")
    chunks = split_numbers(numbers, NUM_workers)
    primes = set()

    #start server
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen()

    print(f"Coordinator started on {HOST}:{PORT}, waiting for {NUM_workers} workers...")

    workers = {}
    worker_id_counter = 0

    task=[]
    for i, chunk in enumerate(chunks):
        task.append({'id':i, 'data':chunk, 'status': 'pending', 'used_worker':None})

    # def_heartbeat_tracker,tine set as 10s
    def heartbeat_tracker():
        while True:
            time.sleep(1)
            now = time.time()
            for wid,info in workers.items():
                if info['alive'] and now - info['last_heartbeat'] < 10:
                    print(f'Worker {wid} is dead')
                    info['alive'] = False
                    for t in task:
                        if t['used_worker'] == wid and t['status'] != 'done':
                            t['status'] = 'pending'
                            t['used_worker'] = None
    threading.Thread(target=heartbeat_tracker).start()

    #connect with worker
     while len(workers) < NUM_workers:
        conn, addr = server.accept()
        print(f"Worker connected from {addr}")
        worker_id_counter+=1
        wid=f'{worker_id_counter}'
        workers[wid] = {'conn':conn,'addr':addr, 'alive':True, 'last_heartbeat':time.time()}
        print(f'Worker {wid} connected from {addr}')

    #deliver task
    for i,(wid,info) in enumerate(workers.items()):
        if i< len(task):
            task=task[i]
            send_data(info['conn'], {'type':'Task','data':task['data'],'task_id':task['task_id'],'status':task['id']})
            task['status']='running'
            task['used_worker'] = wid
            print(f'Worker {wid} do {task['id']}')

    ###Update latter 11.8  main loop_receive task and heart
    completed=0
    while completed<len(task):
        
   

    
    #write results
    with open("outputs/primes_distributed.txt", "w") as f:
        for p in sorted(primes):
            f.write(f"{p}\n")

    print("finished")

if __name__ == "__main__":

    coordinator_main()


