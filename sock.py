import socket


def recv_full(socket: socket.socket, buf_size: int) -> bytes:
    t_bs = bytes()
    while buf_size - len(t_bs) > 0:
        bs = socket.recv(buf_size - len(t_bs))
        if len(bs) == 0:
            break
        t_bs += bs
    return t_bs
