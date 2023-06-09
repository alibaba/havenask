#!/bin/env python
from arpc.proto.rpc_extensions_pb2 import global_service_id, local_method_id, ErrorMsg
from google.protobuf.service import RpcChannel
import socket
import struct

class ANetRpcChannel(RpcChannel):
    def __init__(self, s):
        self.sock = s

    def __del__(self):
        self.sock.close()

    #replace by struct module
    def writeInt32(self, value):
        #make sure value less than 2**32
        value &= 0xFFFFFFFF;
        res = ""
        res = chr((value >> 24) & 0xFF) + \
            chr((value >> 16) & 0xFF) + \
            chr((value >> 8) & 0xFF) + \
            chr(value & 0xFF)
        return res

    def GenerateANetPacketHeader(self, method_descriptor, packetBodyLen):
        anetHeader = ""
        anetFlag = 0x416e4574

        #write anet flag
        anetHeader += struct.pack("!i", anetFlag)

        #write channel id
        channelId = 1
        anetHeader += struct.pack("!i", channelId);

        #write pcode
        serviceId = method_descriptor.containing_service.GetOptions().Extensions[global_service_id];
        methodId = method_descriptor.GetOptions().Extensions[local_method_id]

        pcode = (serviceId << 16) | methodId
        anetHeader += struct.pack("!i", pcode)

        #write datalen
        anetHeader += struct.pack("!i", packetBodyLen)

        return anetHeader
    #done is not used currently
    def CallMethod(self, method_descriptor, rpc_controller, request, response_class, done = None):
        reqPacket = ""
        packetBody = request.SerializeToString()

        reqPacketHeader = self.GenerateANetPacketHeader(method_descriptor, len(packetBody))

        reqPacket = reqPacketHeader + packetBody

        #send request to rpc server
        success = self.SendPacket(reqPacket)
        if not success:
            rpc_controller.SetFailed("send packet error")
            return None

        self.sock.settimeout(30)
        
        response = None
        try:
        #receive response:1.receive header;2.receive body
            buffer = self.sock.recv(struct.calcsize("!iiii"));
            headerTuple = struct.unpack("!iiii", buffer);
            bodyLen = headerTuple[3]
            if bodyLen == 0:
                response = response_class()
                done(response)
                return response
            buffer = ''
            while len(buffer) < bodyLen:
                tmpBuffer = self.sock.recv(bodyLen)
                if not tmpBuffer:
                    break;
                buffer += tmpBuffer

            pcode = headerTuple[2]
            if pcode != 0:
                errMsg = ErrorMsg()
                errMsg.ParseFromString(buffer)
                rpc_controller.SetFailed(errMsg.error_msg)
            else:
                response = response_class()
                response.ParseFromString(buffer)

        except socket.timeout,e:
            rpc_controller.SetFailed("timeout when recv response")
            return None
        done(response)
        return response

    def SendPacket(self, packet):
        bytesSent = 0
        while bytesSent < len(packet):
            sent = self.sock.send(packet[bytesSent:])
            if sent == 0:
                return False
            bytesSent += sent
        return True
            
