import socket
import pickle
import math
import send_data, recv_data


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

    #connect with worker
    workers = []
    while len(workers) < NUM_workers:
        conn, addr = server.accept()
        print(f"Worker connected from {addr}")
        workers.append(conn)
        
    #deliver data
    for i, conn in enumerate(workers):
        send_data(conn, chunks[i])

    #receive results
    for i, conn in enumerate(workers):
        print(f'Receiving from worker{i + 1}')
        try:
            result= recv_data(conn)
            if result:
                primes.update(result)
        except Exception as e:
            print(f'Error receiving from worker{i+1}: {e}')
        finally:
            conn.close()
    #write results
    with open("outputs/primes_distributed.txt", "w") as f:
        for p in sorted(primes):
            f.write(f"{p}\n")

    print("finished")

if __name__ == "__main__":

    coordinator_main()
