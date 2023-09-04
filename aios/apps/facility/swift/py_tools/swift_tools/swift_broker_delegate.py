#!/bin/env python

import arpc.python.rpc_impl_async as rpc_impl
import swift.protocol.Transport_pb2 as swift_proto_transport
import swift.protocol.ErrCode_pb2 as swift_proto_errcode
import swift.protocol.BrokerRequestResponse_pb2 as swift_proto_brokerrequestresponse
import swift_common_define
import swift_delegate_base


class SwiftBrokerDelegate(swift_delegate_base.SwiftDelegateBase):
    FILE_MODE_DATA_SEPARATOR = '\t'

    def __init__(self, username=None, passwd=None, accessId=None, accessKey=None):
        super(SwiftBrokerDelegate, self).__init__(username, passwd, accessId, accessKey)

    def sendMessages(self, serverSpecs, topicName, msgs):
        self.brokerStubs = []
        partId2Reply = {}
        partId2Msgs = self._parseMessages(msgs)
        for partId in partId2Msgs:
            request = swift_proto_brokerrequestresponse.ProductionRequest()
            request.topicName = topicName
            request.partitionId = partId
            pMsgs = partId2Msgs[partId]
            for payload, data in pMsgs:
                msg = request.msgs.add()
                msg.data = data
                msg.uint16Payload = payload
            partId2Reply[partId] = self._callMethod(serverSpecs[partId],
                                                    swift_common_define.SWIFT_METHOD_SEND_MESSAGE,
                                                    request,
                                                    sync=False)
        responses = []
        for partId in partId2Reply:
            partId2Reply[partId].wait(swift_common_define.ARPC_TIME_OUT)
            response = partId2Reply[partId].getResponse()
            ret, errorMsg = self._checkSendMsgResponse(response, serverSpecs[partId])
            if not ret:
                return ret, errorMsg
            responses.append(response)
        return True, responses

    def _parseMessages(self, msgs):
        partId2Msgs = {}
        for msg in msgs:
            pos1 = msg.find(self.FILE_MODE_DATA_SEPARATOR)
            if pos1 == -1:
                return {}
            pos2 = msg.find(self.FILE_MODE_DATA_SEPARATOR, pos1 + 1)
            if pos2 == -1:
                return {}
            partId = int(msg[0:pos1])
            payload = int(msg[pos1 + 1:pos2])
            if partId not in partId2Msgs:
                partId2Msgs[partId] = []
            partId2Msgs[partId].append((payload, msg[pos2 + 1:]))
        return partId2Msgs

    def sendMessage(self, serverSpec, topicName,
                    partId, msgs, payload=None, payloadMode='same',
                    uint8payload=None):
        request = swift_proto_brokerrequestresponse.ProductionRequest()
        request.topicName = topicName
        request.partitionId = partId
        if type(msgs) == type([]):
            for data in msgs:
                msg = request.msgs.add()
                msg.data = data
                if payload is not None:
                    msg.uint16Payload = payload
                    if payloadMode == 'inc':
                        payload += 1
                        payload %= 65536
                if uint8payload is not None:
                    msg.uint8MaskPayload = uint8payload
        else:
            msg = request.msgs.add()
            msg.data = msgs
            if payload is not None:
                msg.uint16Payload = payload
            if uint8payload is not None:
                msg.uint8MaskPayload = uint8payload
        response = self._callMethod(serverSpec,
                                    swift_common_define.SWIFT_METHOD_SEND_MESSAGE,
                                    request)
        ret, errorMsg = self._checkSendMsgResponse(response, serverSpec)
        if not ret:
            return ret, errorMsg
        return True, response

    def _checkSendMsgResponse(self, response, serverSpec):
        if not response:
            return False, "ERROR: response is Empty!"

        if not response.HasField(swift_common_define.PROTO_ERROR_INFO):
            errorMsg = "Response from server[%(server_spec)s] must have field[%(field)s]!" % \
                {"server_spec": serverSpec,
                 "field": swift_common_define.PROTO_ERROR_INFO,
                 }
            return False, errorMsg

        errorInfo = response.errorInfo
        if not errorInfo.HasField(swift_common_define.PROTO_ERROR_CODE):
            errorMsg = "Field[%(error_info)s] of response from "\
                "server[%(server_spec)s] must have field[%(error_code)s]!" %\
                {"error_info": swift_common_define.PROTO_ERROR_INFO,
                 "server_spec": serverSpec,
                 "error_code": swift_common_define.PROTO_ERROR_CODE,
                 }
            return False, errorMsg
        if errorInfo.errCode == swift_proto_errcode.ERROR_BROKER_MSG_LENGTH_EXCEEDED:
            errorMsg = "ErrorCode[%(err_code)d], ErrorMsg[%(err_msg)s],"\
                " maxAllowMsgLen[%(max_allow_msg_len)d]" % \
                {"err_code": errorInfo.errCode,
                 "err_msg": errorInfo.errMsg,
                 "max_allow_msg_len": response.maxAllowMsgLen,
                 }
            return False, errorMsg
        elif errorInfo.errCode != swift_proto_errcode.ERROR_NONE:
            errorMsg = "ErrorCode[%(err_code)d], ErrorMsg[%(err_msg)s]" % \
                {"err_code": errorInfo.errCode,
                 "err_msg": errorInfo.errMsg,
                 }
            return False, errorMsg
        return True, None

    def getMessage(self, serverSpec, topicName, partId, startId,
                   count=None, swiftFilter=None, maxTotalSize=None,
                   fieldNames=None, fieldFilterDesc=None):
        request = swift_proto_brokerrequestresponse.ConsumptionRequest()
        request.topicName = topicName
        request.partitionId = partId
        request.startId = startId
        if count is not None:
            request.count = count

        if swiftFilter is not None:
            request.filter.MergeFrom(swiftFilter)

        if maxTotalSize is not None:
            request.maxTotalSize = maxTotalSize

        if fieldNames is not None:
            fields = fieldNames.split(',')
            for field in fields:
                request.requiredFieldNames.append(field)

        if fieldFilterDesc is not None:
            request.fieldFilterDesc = fieldFilterDesc
        response = self._callMethod(serverSpec,
                                    swift_common_define.SWIFT_METHOD_GET_MESSAGE,
                                    request)
        ret, errorMsg = self._checkErrorInfo(response, serverSpec)
        if not ret:
            return ret, errorMsg
        return True, response

    def getMaxMessageId(self, serverSpec, topicName, partitionId):
        request = swift_proto_brokerrequestresponse.MessageIdRequest()
        request.topicName = topicName
        request.partitionId = partitionId
        response = self._callMethod(serverSpec,
                                    swift_common_define.SWIFT_METHOD_GET_MAX_MESSAGE_ID,
                                    request)
        ret, errorMsg = self._checkErrorInfo(response, serverSpec)
        if not ret:
            return ret, errorMsg
        return True, response

    def getMinMessageIdByTime(self, serverSpec, topicName,
                              partitionId, timestamp):
        request = swift_proto_brokerrequestresponse.MessageIdRequest()
        request.topicName = topicName
        request.partitionId = partitionId
        request.timestamp = int(timestamp)
        response = self._callMethod(serverSpec,
                                    swift_common_define.SWIFT_METHOD_GET_MIN_MESSAGE_ID_BY_TIME,
                                    request)
        ret, errorMsg = self._checkErrorInfo(response, serverSpec)
        if not ret:
            return ret, errorMsg
        return True, response

    def _callMethod(self, serverSpec, method, request, sync=True):
        request.authentication.username = self.username
        request.authentication.passwd = self.passwd
        request.authentication.accessAuthInfo.accessId = self.accessId
        request.authentication.accessAuthInfo.accessKey = self.accessKey
        request.versionInfo.supportAuthentication = True
        brokerStub = rpc_impl.ServiceStubWrapperFactory.createServiceStub(
            swift_proto_transport.Transporter_Stub,
            serverSpec)
        if not brokerStub:
            print "ERROR: CreateServiceStub failed!"
            return None
        try:
            if sync:
                response = brokerStub.syncCall(method,
                                               request,
                                               timeOut=swift_common_define.ARPC_TIME_OUT)
                return response
            else:
                self.brokerStubs.append(brokerStub)
                reply = brokerStub.asyncCall(method, request)
                return reply

        except rpc_impl.ReplyTimeOutError as e:
            print "ERROR: call method[%s] in server[%s] time out!"\
                % (method, serverSpec)
            return None
