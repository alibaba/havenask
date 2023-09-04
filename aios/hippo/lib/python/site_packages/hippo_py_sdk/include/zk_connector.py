import sys
import os
import new
import time, threading
import traceback

currentPath = os.path.split(os.path.realpath(__file__))[0]
zookeeperMtLibPath = os.path.join(currentPath, '../../../../../../lib64/')
if not os.environ.has_key('LD_LIBRARY_PATH'):
    os.environ['LD_LIBRARY_PATH'] = ''
os.environ['LD_LIBRARY_PATH'] = zookeeperMtLibPath + ':' + os.environ['LD_LIBRARY_PATH']
import platform
version = platform.python_version()
import zookeeper_for_py2_7.zookeeper as zookeeper

zookeeper.set_debug_level(zookeeper.LOG_LEVEL_WARN)
count = 0
zkref = None
DEBUG_MODE = False

class Logger:
    @staticmethod
    def printinfo(*var):
        global DEBUG_MODE
        if DEBUG_MODE:        
            print var
        
#each thead should create one connector

zk_connectors = {}
lock = threading.Lock()

def parse_zk_path(zk_path):
    if zk_path.startswith('zfs://'):
        zk_server = zk_path[6:]
    else:
        zk_server = zk_path
    idx = zk_server.find('/')
    path = ''
    if idx != -1:
        path = zk_server[idx:]
        zk_server = zk_server[:idx]
    return zk_server, path

def getZkConnector(zk_server, time_out = None):
    global lock
    global zk_connectors
    lock.acquire()
    zk_connector = None
    try:
        if zk_connectors.has_key(zk_server):
            zk_connector = zk_connectors[zk_server]
        else:
            zk_connector = ZkConnector(True)
            zk_connectors[zk_server] = zk_connector
        if not zk_connector.isOk():
            is_connected = zk_connector.connect(zk_server, time_out)
            if not is_connected:
                zk_connector = None
    except:
        traceback.print_exc()
    lock.release()
    if zk_connector:
        zk_connector.refCnt += 1
    return zk_connector

class ZkConnector:
    @staticmethod
    def CheckAndReConnect(zk_con):
        if None == zk_con:
            raise "zk_con is None"
        Logger.printinfo("DEBUG: ","zk_con:" ,zk_con, " handle: ", zk_con.handle, " isOk: ", zk_con.isOk())
        if not zk_con.isOk():
            if not zk_con.reConnect():                                   
                raise "zk con is not ok"  
    def __init__(self, verbose = False):
        global zkref
        zkref = self
        self.connected = False
        self.bad = False
        self.verbose = True
        self.signalGetList = set([])
        self.signalExistList = set([])
        self.conn_cv = threading.Condition()
        self.zk_server = None
        self.handle = None
        self.refCnt = 0
        self.acl = {"perms":zookeeper.PERM_ALL, "scheme":"world", "id" :"anyone"}

    def __del__(self):
        if self.connected:
            self.close()
        self.connected = False
        self.bad = False   

    def isOk(self):
        return self.connected and not self.bad

    def setGetList(self, path):
        self.conn_cv.acquire()
        self.signalGetList.add(path)
        self.conn_cv.notifyAll()
        self.conn_cv.release()
        
    def setExistList(self, path):
        self.conn_cv.acquire()
        self.signalExistList.add(path)
        self.conn_cv.notifyAll()
        self.conn_cv.release()
        
    def hasSignal(self):
        self.conn_cv.acquire()
        sz = len(self.signalGetList) + len(self.signalExistList)
        self.conn_cv.release()
        return sz > 0

    def getGetList(self):
        self.conn_cv.acquire()
        getList = self.signalGetList.copy()
        self.signalGetList.clear()
        self.conn_cv.release()
        return getList
        
    def getExistList(self):
        self.conn_cv.acquire()
        existList = self.signalExistList.copy()
        self.signalExistList.clear()
        self.conn_cv.release()
        return existList

    def getList(self):
        return self.getGetList() | self.getExistList()

    def waitSignal(self, timeout = None):
        begin = time.time()
        sz = 0
        self.conn_cv.acquire()
        while self.connected:
            sz = len(self.signalGetList) + len(self.signalExistList)
            if sz > 0:
                break
            if timeout != None:
                if time.time() - begin > timeout:
                    break
            self.conn_cv.wait(1)
        self.conn_cv.release()
        return sz > 0 or not self.connected

    def lsdir(self, path):
        if not path.startswith("/"):
            return False, []
        subnodes = []
        try:
            subnodes = zookeeper.get_children(self.handle, path)
        except zookeeper.NoNodeException:
            if self.verbose:
                Logger.printinfo("DEBUG: ", path, " not found")
                return False, []
        return True, subnodes
        
    def rmdir(self, path):
        if not path.startswith("/"):
            return False
        ret, subnodes = self.lsdir(path)
        if not ret:
            return True
        for s in subnodes:
            self.rmdir(path + "/" + s)
        try:
            if self.verbose:
                Logger.printinfo("DEBUG: deleting ", path)
            zookeeper.delete(self.handle, path)
        except zookeeper.NoNodeException:
            if self.verbose:
                Logger.printinfo("DEBUG: ", path, " not found")
        return True

    def mkdir(self, path):
        if not path.startswith("/"):
            return False
        ps = path.split("/")
        history = ""
        for p in ps:
            if len(p) == 0:
                continue
            curr = history + "/" + p
            try:
                if not self.exist(curr):
                    zookeeper.create(self.handle, curr, "", [self.acl], 0)
            except zookeeper.NodeExistsException:
                pass
            history = curr
        return True

    def get(self, path, watch = False):
        def get_watcher(handle,type,state,path):
        #def get_watcher(*arg):
            global count,zkref
            count = count + 1
            Logger.printinfo("INFO:zkref:",zkref," count",count," handle",handle," type",type," state",state," path",path)
            if zkref.verbose:
                Logger.printinfo("DEBUG: node ", path, " changed, handle is ", handle, "event is ", type)
            path.strip()
            if len(path) > 0:
                zkref.setGetList(path)

        if self.verbose:
            Logger.printinfo("DEBUG: getting value from ", path)
        try:
            if watch:
                if path in self.signalGetList:
                    Logger.printinfo("ERROR: ", path, " already in signal list")
                value = zookeeper.get(self.handle, path, get_watcher)
            else:
                value = zookeeper.get(self.handle, path)
            if self.verbose:
                Logger.printinfo("DEBUG: current value is ", value)
            return True, value[0]
        except zookeeper.NoNodeException:
            if self.verbose:
                Logger.printinfo("DEBUG: node ", path, "is not exist")
        return False, 0
    #True,'''{'pzxid': 158914164912L, 'ctime': 1302659362784L, 'aversion': 0, 'mzxid': 455292014174L, 'numChildren': 0, 'ephemeralOwner': 0L, 'version': 241, 'dataLength': 3, 'mtime': 1328509095562L, 'cversion': 0, 'czxid': 158914164912L}'''
    def stat(self, path, watch = False):
        def get_watcher(handle,type,state,path):
            if self.verbose:
                Logger.printinfo("DEBUG: node ", path, " changed, handle is ", handle, "event is ", type)
            path.strip()
            if len(path) > 0:
                self.setGetList(path)

        if self.verbose:
            Logger.printinfo("DEBUG: getting value from ", path)
        try:
            if watch:
                if path in self.signalGetList:
                    Logger.printinfo("ERROR: ", path, " already in signal list")
                value = zookeeper.get(self.handle, path, get_watcher)
            else:
                value = zookeeper.get(self.handle, path)
            if self.verbose:
                Logger.printinfo("DEBUG: current value is ", value)
            return True, value[1]
        except zookeeper.NoNodeException:
            if self.verbose:
                Logger.printinfo("DEBUG: node ", path, "is not exist")
        return False, {}
       
    def exist(self, path, watch = False):
        def exist_watcher(handle,type,state,path):
            if self.verbose:
                Logger.printinfo("DEBUG: node ", path, " exist change, handle is ", handle, "event is ", type)
            self.setExistList(path)

        if self.verbose:
            Logger.printinfo("DEBUG: checking exist for ", path)
        try:
            if watch:
                if path in self.signalExistList:
                    Logger.printinfo("ERROR: ", path, " already in signal list")
                value = zookeeper.exists(self.handle, path, exist_watcher)
            else:
                value = zookeeper.exists(self.handle, path)
            if self.verbose:
                Logger.printinfo("DEBUG: current value is ", value)
            return value != None
        except zookeeper.NoNodeException:
            if self.verbose:
                Logger.printinfo("DEBUG: node ", path, "is not exist")
        return False
    
    def set(self, path, value):
        if self.verbose:
            Logger.printinfo("DEBUG: setting value ", value, " to ", path)
        try:
            zookeeper.set(self.handle, path, value)
        except zookeeper.NoNodeException:
            if self.verbose:
                Logger.printinfo("DEBUG: create & setting value ", value, " to ", path)
            return self.create(path, value)
        return True

    def create(self, path, value = ""):
        pos = path.rfind('/')
        dir_path = path[:pos]
        # make sure the directory created
        if not self.exist(dir_path):
            if not self.mkdir(dir_path):
                return False
        try:
            zookeeper.create(self.handle, path, value, [self.acl], 0)
        except Exception, e:
            if (str(e) == 'node exists'):
                print "WARN: node already exists, path: %s" % path
                return True
            print "ERROR: create node fail:, %s, Exception: %s" % (path, e)
            return False
        return True 


    def createES(self, path, value = ""):
        if self.verbose:
            Logger.printinfo("DEBUG: creating ", path)
        print self.acl
        realnode = zookeeper.create(self.handle, path, value, [self.acl], zookeeper.EPHEMERAL | zookeeper.SEQUENCE)
        if self.verbose:
            Logger.printinfo("DEBUG: ", realnode, " created")
        return realnode

    def createES_NOSE(self, path, value = ""):
        if self.verbose:
            Logger.printinfo("DEBUG: creating ", path)
        print self.acl                                              
        realnode = zookeeper.create(self.handle, path, value, [self.acl], zookeeper.EPHEMERAL)
        if self.verbose:
            Logger.printinfo("DEBUG: ", realnode, " created")
        return realnode


    def setlog(self, log):
        f = open(log, "w")
        zookeeper.set_log_stream(f)

    def setZkServer(self, zkserver):
        self.zk_server = zkserver
    
    def connect(self, zkserver, timeout = None):
        def connection_watcher(handle,type,state,path):
            try:
                if state == zookeeper.CONNECTED_STATE:
                    if self.verbose:
                        Logger.printinfo("DEBUG: Connected, handle is ", handle, "state is ", state)
                    self.conn_cv.acquire()
                    self.connected = True
                    self.conn_cv.notifyAll()
                    self.conn_cv.release()
                else: 
                    if self.verbose:
                        Logger.printinfo("DEBUG: Disconnected, handle is ", handle, "state is ", state)
                    self.conn_cv.acquire()
                    self.connected = False
                    self.bad = True
                    self.conn_cv.notifyAll()
                    self.conn_cv.release()
            except:
                 Logger.printinfo("ERROR: connection_watcher exception occurs")
                 traceback.print_exc()
                 

        if self.verbose:
            Logger.printinfo("DEBUG: Connecting to ", zkserver)
        self.connected = False
        self.bad = False
        self.zk_server = zkserver
        try:
            self.handle = zookeeper.init(zkserver, connection_watcher, 100000)
            begin = time.time()
            self.conn_cv.acquire()
            while not self.connected:
                if timeout != None:
                    if time.time() - begin > timeout:
                        self.conn_cv.release()                        
                        zookeeper.close(self.handle)
                        self.conn_cv.acquire()           
                        self.connected = False
                        break
                self.conn_cv.wait(1)
            self.conn_cv.release()
        except:
            Logger.printinfo("ERROR: connect failed, ",  sys.exc_info()[0])
        Logger.printinfo("DEBUG: Connected zk handle: ", self.handle, " connector: ", self)
        if self.verbose:
            if self.connected:
                Logger.printinfo("DEBUG: Connected")
            else:
                Logger.printinfo("DEBUG: Not Connected")
        return self.connected

    def reConnect(self, timeout = None):
        if None == self.zk_server:
            Logger.printinfo("ERROR: reconnect failed, no zk_server")
            return False
        return self.connect(self.zk_server, timeout)

    def close(self):
        try:
            if self.verbose:
                Logger.printinfo("DEBUG: handle ", self.handle, " is closed")
            if self.refCnt > 1:
                self.refCnt -= 1
                return
            zookeeper.close(self.handle)
            self.connected = False
        except:
            if self.verbose:
                Logger.printinfo("DEBUG: close failed, ",  sys.exc_info()[0])


    
    
    



