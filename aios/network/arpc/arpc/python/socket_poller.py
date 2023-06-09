#!/bin/env python
import threading
import select
import time
import connection_manager

STATUS_STARTED = 0
STATUS_STOPPING = 1
STATUS_STOPPED = 2

gConnMgr = connection_manager.ConnectionManager()

class SocketPoller:
    def __init__(self):
        self.pollThread = None
        self.lock = threading.RLock()
        self.status = STATUS_STOPPED
        pass

    def start(self):
        self.lock.acquire()
        if self.status == STATUS_STARTED:
            self.lock.release()
            return True
        elif self.status == STATUS_STOPPING:
            self.lock.release()
            return False

        self.pollThread = threading.Thread(target = self.pollLoop)
        try:
            self.pollThread.setDaemon(True)
            self.pollThread.start()
        except RuntimeException:
            print "ERROR: start thread failed"
            self.lock.release()
            return False

        self.status = STATUS_STARTED
        self.lock.release()
        return True

    def stop(self):
        self.lock.acquire()
        if self.status != STATUS_STARTED:
            self.lock.release()
            return

        self.status = STATUS_STOPPING
        self.lock.release()

        if self.pollThread != None:
            self.pollThread.join()
        self.status = STATUS_STOPPED

    def register(self, conn):
        gConnMgr.addConn(conn)

    def pollLoop(self):
        while True:
            self.lock.acquire()
            status = self.status
            self.lock.release()
            if status != STATUS_STARTED:
                break

            poll = select.poll()
            gConnMgr.registerPollEventForAll(poll)
            try:
                ret = poll.poll(10)
                for (fd, event) in ret:
                    gConnMgr.handleEvent(fd, event)
            except Exception, msg:
                pass

class SocketPollerShell:
    def __init__(self, sp):
        self.sp = sp

    def __del__(self):
        self.sp.stop()
