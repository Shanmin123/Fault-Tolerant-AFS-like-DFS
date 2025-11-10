import socket
import pickle
import math


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
    numbers = read_numbers("data/test11.txt")
    chunks = split_numbers(numbers, NUM_workers)
    primes = set()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((HOST, PORT))
    server.listen()

    print(f"Coordinator started on {HOST}:{PORT}, waiting for {NUM_workers} workers...")

    workers = []
    while len(workers) < NUM_workers:
        conn, addr = server.accept()
        print(f"Worker connected from {addr}")
        workers.append(conn)

    for i, conn in enumerate(workers):
        task = pickle.dumps(chunks[i])
        conn.sendall(task)


    for conn in workers:
        data = conn.recv(4096)
        result = pickle.loads(data)
        primes.update(result)
        conn.close()

    with open("outputs/primes_distributed1.txt", "w") as f:
        for p in sorted(primes):
            f.write(f"{p}\n")

    print("finished")

if __name__ == "__main__":
    coordinator_main()