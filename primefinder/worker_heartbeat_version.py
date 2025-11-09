import socket
import pickle
from prime_test import prime_test
from send_recv import send_data, recv_data
import time
import threading
HOST = 'localhost'
PORT = 5000

#send heartbeat info
def heartbeat(conn):
    while True:
        time.sleep(1)
        try:
            send_data(conn, {'type': 'heartbeat'})
        except Exception as e:
            print(e)

def worker_main():
    s=socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    print('Connected to coordinator')
    
    #start heartbeat
    threading.Thread(target=heartbeat, args=(s,)).start()

    #receive message
    while True:
        msg= recv_data(s)
        if not msg:
            continue
        if msg['type'] == 'Task':
            data=msg['data']
            task_id=msg['task_id']
            print('Received task:', task_id)
            primes= []
            for i in data:
                if prime_test(i):
                    primes.append(i)
            
            send_data(s, {'type': 'Result', 'task_id': task_id, 'data': primes})
            print(f'Task {task_id} completed successfully')
        else:
            time.sleep(1)
            print('Received unknown task:', msg)
if __name__ == "__main__":
    worker_main()



