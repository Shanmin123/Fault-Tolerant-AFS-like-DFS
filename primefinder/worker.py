import socket
import pickle
from prime_test import prime_test
from send_recv import send_data, recv_data

HOST = 'localhost'
PORT = 5000

def worker_main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    print("Connected to coordinator")

    numbers = recv_data(s)
    primes=[]
    for i in numbers:
        if prime_test(i):
            primes.append(i)
    send_data(s, primes)
    s.close()
if __name__ == "__main__":
    worker_main()


