import socket
from contextlib import closing

class PortUtil(object):
    @staticmethod
    def get_unused_port():
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(('localhost', 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

    @staticmethod
    def get_unused_ports(num):
        sockets = []
        ports = []
        for i in range(num):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(('localhost', 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            ports.append(s.getsockname()[1])
            sockets.append(s)
        for s in sockets:
            s.close()
        return ports

