import socket
import pickle

def send_data(conn, data_part):
    load=pickle.dumps(data_part)
    data_len = len(load).to_bytes(8, 'big')
    conn.sendall(data_len)
    conn.sendall(load)

def recv_data(conn):
    data_len_bytes = conn.recv(8)
    data_len = int.from_bytes(data_len_bytes, 'big')
    data= b''
    while len(data) < data_len:
        data += conn.recv(4096)
    return pickle.loads(data)
