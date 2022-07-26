import socket
import threading


class sock(socket.socket):
    def __init__(self, family=-1, type=-1, proto=-1, fileno=None):
        super(sock, self).__init__(family, type, proto, fileno)
        self.down_event = threading.Event()

    def close(self) -> None:
        self.down_event.set()
        super(sock, self).close()

    def recv(self, bufsize: int, flags: int = ...) -> bytes:
        try:
            return super(sock, self).recv(bufsize, flags)
        except BaseException as e:
            self.down_event.set()
            raise e

    def send(self, data: bytes, flags: int = ...) -> int:
        try:
            return super(sock, self).send(data, flags)
        except BaseException as e:
            self.down_event.set()
            raise e

    def accept(self):
        try:
            super(sock, self).accept()
        except BaseException as e:
            self.down_event.set()
            raise e

    def listen(self, __backlog: int = ...) -> None:
        try:
            if __backlog is Ellipsis:
                super(sock, self).listen()
            else:
                super(sock, self).listen(__backlog)
        except BaseException as e:
            self.down_event.set()
            raise e

    def bind(self, address) -> None:
        try:
            super(sock, self).bind(address)
        except BaseException as e:
            self.down_event.set()
            raise e
