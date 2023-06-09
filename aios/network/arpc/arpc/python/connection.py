#!/bin/env python

from arpc.proto.rpc_extensions_pb2 import global_service_id, local_method_id, ErrorMsg
import threading
import socket
import struct
import errno

READ_LEN = 1024 * 4

class Connection:
    def __init__(self, spec):
        self._spec = spec
        self._sock = None
        self._isConned = None
        self._packetMap = {}
        self._maxChannelId = 0
        self._lock = threading.Lock()
        self._isClosed = True
        self._isStarted = False
        self._outbuf = ''
        self._inbuf = ''
        self._createSock()

    def _parseSpec(self, spec):
        argList = spec.split(':');
        if len(argList) != 3:
            print "ERROR:server address format is {tcp|udp}:ip:port, example \"tcp:127.0.0.1:9876\""
            return (None, None)
        return (argList[1].strip(), argList[2].strip())

    def _createSock(self):
        #create socket and set non-blocking
        try:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
            self._isClosed = False
            self._sock.setblocking(0)
        except socket.error, msg:
            print "ERROR: create socket exception %s" % msg
            return

    def isConnected(self):
        isConned = None
        self._lock.acquire()
        isConned = self._isConned
        self._lock.release()
        return isConned

    def setConnected(self, flag):
        self._lock.acquire()
        self._isConned = flag
        self._lock.release()
    
    def connect(self):
        if self.isConnected() == True:
            return True

        #connect to server
        (ip, port) = self._parseSpec(self._spec)
        if (ip == None or ip == '') or (port == None or port == ''):
            print "connect ip or port error"
            return False

        if self._sock == None:
            return False

        try:
            self._sock.connect((ip, int(port)))
        except socket.error, error:
            if error[0] != errno.EINPROGRESS:
                print "ERROR: connect to %s:%s failed, %s" % (ip, port, 
                                                              error)
                self.close()
                return False
        except Exception, error:
            print "ERROR:", error
            self.close()
            return False

        self._isStarted = True
        return True

    def isClosed(self):
        return self._isClosed

    def close(self):
        if self._isClosed:
            return
        self._sock.close();
        self._isClosed = True
        pass

    def atomicGetGlobalChannelId(self):
        self._lock.acquire()
        self._maxChannelId += 1
        channelId = self._maxChannelId
        self._lock.release()
        return channelId

    def post(self, methodDescriptor, request, context):
        channelId, reqPacket = self.generateAnetPacket(methodDescriptor, 
                                                       request)
        self.postPacket(reqPacket, channelId, context)
        return True
    
    def postPacket(self, packet, channelId, context):
        self._lock.acquire()
        self._packetMap[channelId] = context
        self._outbuf += packet
        self._lock.release()

    def generateAnetPacket(self, methodDescriptor, request):
        packetBody = request.SerializeToString()
        channelId = self.atomicGetGlobalChannelId()
        pcode = self.generateANetPacketCode(methodDescriptor)
        reqPacketHeader = self.generateANetPacketHeader(channelId, pcode, 
                                                        len(packetBody))
        reqPacket = reqPacketHeader + packetBody
        return (channelId, reqPacket)

    def generateANetPacketCode(self, methodDescriptor):
        serviceId = methodDescriptor.containing_service.GetOptions().Extensions[global_service_id];
        methodId = methodDescriptor.GetOptions().Extensions[local_method_id]

        pcode = (serviceId << 16) | methodId
        return pcode
        
    def generateANetPacketHeader(self, channelId, pcode, packetBodyLen):
        anetHeader = ""
        anetFlag = 0x416e4574

        #write anet flag
        anetHeader += struct.pack("!i", anetFlag)

        #write channel id
        anetHeader += struct.pack("!i", channelId);

        #write pcode
        anetHeader += struct.pack("!i", pcode)

        #write datalen
        anetHeader += struct.pack("!i", packetBodyLen)

        return anetHeader

    def handleWrite(self):
        if not self.isConnected():
            self.setConnected(True)
        self._writeDataFromOutBuffer()

    def handleRead(self):
        self._readDataToInBuffer()
        while self._handlePacket():
            pass

    def _writeDataFromOutBuffer(self):
        errorCode = errno.EAGAIN
        errorMsg = ''

        self._lock.acquire()
        if len(self._outbuf) != 0:
            try:
                sendLen = self._sock.send(self._outbuf)
                self._outbuf = self._outbuf[sendLen:]
            except Exception, error:
                errorCode = error[0]
                errorMsg = error[1]
        self._lock.release()

        if errorCode not in [errno.EAGAIN, errno.EWOULDBLOCK]:
            self._failAll(str(errorMsg))
                
    def _readDataToInBuffer(self):
        try:
            buf = self._sock.recv(READ_LEN)
            if len(buf) == 0:
                self.close()
                self._failAll("connection was closed by remote")
            else:
                self._inbuf += buf
        except socket.timeout, error:
            self._failAll(str(error[1]))
        except Exception, error:
            if error[0] in [errno.EAGAIN, errno.EWOULDBLOCK]:
                return
            self._failAll(str(error[1]))

    def _failAll(self, msg):
        self._lock.acquire()
        for k, v in self._packetMap.items():
            v.done._controller.SetFailed("rpc failed: %s" % msg)
            v.done()
        self._lock.release()

    def _readPacket(self):
        bufLen = len(self._inbuf)
        headLen = struct.calcsize("!iiii")
        if headLen > bufLen:
            return (None, None)
        
        headBuf = self._inbuf[0:headLen]
        headerTuple = None
        try:
            headerTuple = struct.unpack("!iiii", headBuf);
        except Exception, msg:
            errorMsg = "unpack packet head [%s] failed: %s" % (headBuf, msg)
            print errorMsg
            self._failAll(str(errorMsg))

        if headerTuple == None:
            return (None, None)

        bodyLen = headerTuple[3]
        packetLen = headLen + bodyLen
        if packetLen > bufLen:
            return (None, None)
            
        body = self._inbuf[headLen:packetLen]
        self._inbuf = self._inbuf[packetLen:]
        return (headerTuple, body)
        
    def _handlePacket(self):
        headerTuple, packetBody = self._readPacket()
        if headerTuple == None:
            return False

        channelId = headerTuple[1]
        if channelId not in self._packetMap:
            print "channelId [%d] not in packetMap, size [%d]" % (channelId, len(self._packetMap))
            return True

        context = self._packetMap[channelId]
        callback = context.done

        pcode = headerTuple[2]
        if pcode != 0:
            callback._controller.SetFailed("wrong content")
            callback()
            return True

        response = context.responseClass()
        if len(packetBody) != 0:
            response.ParseFromString(packetBody)
        callback(response)
        return True

    def writable(self):
        if self._isStarted and not self.isConnected():
            return True
        
        ret = False
        self._lock.acquire()
        if len(self._outbuf) != 0:
            ret = True
        self._lock.release()
        return ret




         
