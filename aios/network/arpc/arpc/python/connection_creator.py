#!/bin/env python
import connection
import socket_poller

socketPoller = socket_poller.SocketPoller()
sps = socket_poller.SocketPollerShell(socketPoller)

class ConnectionCreator:
    @classmethod
    def createConnection(self, spec):
        conn = connection.Connection(spec)
        if conn._sock == None:
            return None

        socketPoller.register(conn)
        socketPoller.start()
        
        return conn
        
