#!/bin/env python
import threading
import select

class ConnectionManager:
    def __init__(self):
        self.lock = threading.RLock()
        self.connMap = {}

    def addConn(self, conn):
        if conn._sock == None:
            return
        fd = conn._sock.fileno()

        self.lock.acquire()
        self.connMap[fd] = conn
        self.lock.release()

    def delConn(self, conn):
        if conn._sock == None:
            return
        fd = conn._sock.fileno()
        delConnByFD(fd)

    def delConnByFD(self, fd):
        self.lock.acquire()
        if fd in self.connMap:
            del(self.connMap[fd])
        self.lock.release()
    
    def getCount(self, conn):
        count = 0
        self.lock.acquire()
        count = len(self.connMap)
        self.lock.release()
        return count

    def getConn(self, fd):
        ret = None
        self.lock.acquire()
        if fd in self.connMap:
            ret = self.connMap[fd]
        self.lock.release()
        return ret

    def registerPollEventForAll(self, poll):
        closedFds = []
        self.lock.acquire()

        for fd in self.connMap:
            conn = self.connMap[fd]
            if conn.isClosed():
                closedFds.append(fd)
                continue
            event = select.POLLIN
            if conn.writable():
                event = event | select.POLLOUT
            poll.register(fd, event)

        # remove closed connection
        for fd in closedFds:
            del(self.connMap[fd])

        self.lock.release()

    def handleEvent(self, fd, event):
        self.lock.acquire()
        if fd not in self.connMap:
            self.lock.release()
            return

        conn = self.connMap[fd]
        if event & select.POLLIN:
            conn.handleRead()
        if event & select.POLLOUT:
            conn.handleWrite()
        if event & select.POLLHUP:
            if conn.isConnected():
                del self.connMap[fd]
        self.lock.release()
        return

