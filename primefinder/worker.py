import socket
import pickle
from prime import is_prime

HOST = 'localhost'
PORT = 5000

def worker_main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    print("Connected to coordinator")

    data = s.recv(4096)
    numbers = pickle.loads(data)


    primes = {n for n in numbers if is_prime(n)}

    s.sendall(pickle.dumps(primes))
    s.close()

if __name__ == "__main__":
    worker_main()
