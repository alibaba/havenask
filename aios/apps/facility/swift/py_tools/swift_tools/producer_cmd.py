#!/bin/env python

import os
import swift_common_define
import local_file_util
import swift_util
import transport_cmd_base
import swift.protocol.ErrCode_pb2 as swift_proto_errcode


class ProducerCmd(transport_cmd_base.TransportCmdBase):
    '''
    swift {sm|sendmsg}
       {-z zookeeper_address  | --zookeeper=zookeeper_address}
       {-t topic_name         | --topic=topic_name}
       {-p partition_id       | --partid=partition_id}
       [-m msgs_file          | --msgfile=msgfile]
       [-s msgs_separator     | --separator=msgs_separator]
       [                      | --payload=payload]
       [                      | --payloadmode=payload_mode]
       [                      | --uint8payload=payload]
       [-n                    | --nosep]
       ["msg1", "msg2" ]

    options:
       -z zookeeper_address, --zookeeper=zookeeper_address   : required, zookeeper root address
       -t topic_name,        --topic=topic_name              : required, topic name, eg: news
       -p partition_id,      --partid=partition_id           : required, partition id in a topic
       -m msgs_file          --msgfile=msgfile               : optional, file contains msgs
       -s msgs_separator     --separator=msgs_separator      : optional, msgs separator in file, default is "\\n"
                                                               used with -m option
       --payload payload                                     : optional, set msg payload
       --payloadmode payload_mode                            : optional, set msg payload mode, [same, inc, file],default is same.
       --uint8payload                                        : optional, set uint8 payload for mask.
       -n ,                  --nosep                         : optional, not add seperator at the end of msg,
                                                               used with -m option
       "msg1", "msg2"                                        : optional, messages to send, sperated by space
                                                               one of msgfile and msg must be specified,
                                                               if both of them are specified, use msg.


Example:
    swift sm -z zfs://10.250.12.23:1234/root -t news -p 1 "msg1" "msg2" "msg3"
    swift sm -z zfs://10.250.12.23:1234/root -t news -p 1 --payload 3 --payloadmode "inc" --uint8payload 10 "msg1" "msg2" "msg3"
    swift sm -z zfs://10.250.12.23:1234/root -t news -p 1 -m /path/to/msg_file -s "\\n"
    swift sm -z zfs://10.250.12.23:1234/root -t news -p 1 -m /path/to/msg_file -s "\\n" -n
    '''
    FILE_MODE = 'file'

    def addOptions(self):
        super(ProducerCmd, self).addOptions()
        self.parser.add_option('-t', '--topic', action='store', dest='topicName')
        self.parser.add_option('-p', '--partid', type='int', action='store', dest='partId')
        self.parser.add_option('-m', '--msgfile', action='store', dest='msgFile')
        self.parser.add_option('-s', '--separator', action='store', dest='msgSeparator', default='\n')
        self.parser.add_option('-n', '--nosep', action='store_true', dest='msgWithoutSep', default=False)
        self.parser.add_option('-g', '--msgs', action='store', dest='msgs')
        self.parser.add_option('', '--payload', type='int', action='store', dest='payload')
        self.parser.add_option('', '--payloadmode', action='store', dest='payloadMode',
                               choices=['same', 'inc', self.FILE_MODE], default='same')
        self.parser.add_option('', '--uint8payload', type='int', action='store', dest='uint8payload')

    def initMember(self, options):
        super(ProducerCmd, self).initMember(options)
        if options.topicName is not None:
            self.topicName = options.topicName

        if options.partId is not None:
            self.partId = options.partId
        else:
            self.partId = None

        if options.msgFile is not None:
            self.msgFile = options.msgFile

        if options.msgSeparator is not None:
            self.msgSeparator = options.msgSeparator

        self.msgs = []
        if options.msgs is not None:
            msgStr = options.msgs.encode('ascii', 'ignore')
            self.msgs = msgStr.split('@')

        self.payload = None
        if options.payload is not None:
            self.payload = options.payload

        self.payloadMode = None
        if options.payloadMode is not None:
            self.payloadMode = options.payloadMode

        self.uint8payload = None
        if options.uint8payload is not None:
            self.uint8payload = options.uint8payload

        self.msgWithoutSep = options.msgWithoutSep

        if len(self.args) > 0:
            self.msgs = self.args

    def checkOptionsValidity(self, options):
        ret, errMsg = super(ProducerCmd, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg

        if options.topicName is None:
            errMsg = "ERROR: topic must be specified!"
            print errMsg
            return False, errMsg

        if options.partId is None and options.payloadMode != self.FILE_MODE:
            errMsg = "ERROR: partid must be specified!"
            print errMsg
            return False, errMsg

        if options.msgFile is None and len(self.args) == 0 and len(options.msgs) == 0:
            errMsg = "ERROR: msgfile or msg must be specified!"
            print errMsg
            return False, errMsg

        return True, errMsg

    def unquoteCharacter(self, string):
        escapeCharsDict = {'\\b': '\b',
                           '\\n': '\n',
                           '\\f': '\f',
                           '\\r': '\r',
                           '\\t': '\t',
                           '\\v': '\v',
                           '\\\\': '\\',
                           '\\?': '\\?',
                           "\\'": '\'',
                           '\\"': '\"',
                           "\\0": '\0',
                           }
        for key, value in escapeCharsDict.items():
            pos = string.find(key)
            if pos >= 0:
                string = string.replace(key, value)
        return string

    def _readMsgFromFile(self):
        if not os.path.exists(self.msgFile):
            print "ERROR: msgfile[%s] does not exist!" % self.msgFile
            return False

        localFileUtil = local_file_util.LocalFileUtil()
        content = localFileUtil.read(self.msgFile)
        if not content:
            print "ERROR: msgfile[%s] is empty!" % self.msgFile
            return False

        self.msgSeparator = self.unquoteCharacter(self.msgSeparator)
        self.msgs = swift_util.SwiftUtil.splitStr(content,
                                                  self.msgSeparator,
                                                  saveSep=(not self.msgWithoutSep),
                                                  stripItem=False)
        if len(self.msgs) == 0:
            print "ERROR: msgfile[%s] has no msg!" % self.msgFile
            return False

        return True

    def run(self):
        response = swift_proto_errcode.CommonResponse()
        response.errorInfo.errCode = swift_proto_errcode.ERROR_UNKNOWN
        brokerAddr = self.getBrokerAddressFromAdmin(self.topicName,
                                                    self.partId)
        if not brokerAddr:
            if self.payloadMode == self.FILE_MODE:
                response.errorInfo.errMsg = "ERROR: getBrokerAddressFromAdmin for topic[%s], "\
                                            "partitions faild!" % (self.topicName)
            else:
                response.errorInfo.errMsg = "ERROR: getBrokerAddressFromAdmin for topic[%s], "\
                    "partition id[%d] faild!" % (self.topicName, self.partId)
            print response.errorInfo.errMsg
            return "", response, 1
        if len(self.args) == 0 and len(self.msgs) == 0:
            if not self._readMsgFromFile():
                response.errorInfo.errMsg = "ERROR: read msg from file failed"
                return "", response, 1
        if self.payloadMode == self.FILE_MODE:
            ret, data = self.brokerDelegate.sendMessages(brokerAddr,
                                                         self.topicName,
                                                         self.msgs)
        else:
            ret, data = self.brokerDelegate.sendMessage(brokerAddr,
                                                        self.topicName,
                                                        self.partId,
                                                        self.msgs,
                                                        self.payload,
                                                        self.payloadMode,
                                                        self.uint8payload)
        if not ret:
            print "ERROR: send message failed!"
            print data
            response.errorInfo.errMsg = data
            return ret, response, 1
        print "Send message success!"
        if self.fromApi:
            return ret, data, 0
        responses = data
        if type(responses) != type([]) and responses.HasField(swift_common_define.PROTO_ACCEPTED_MSG_COUNT):
            print "AcceptedMsgCount: %d" % responses.acceptedMsgCount
        elif type(responses) == type([]):
            acceptedMsgCount = 0
            for response in responses:
                if response.HasField(swift_common_define.PROTO_ACCEPTED_MSG_COUNT):
                    acceptedMsgCount += response.acceptedMsgCount
                if response.HasField(swift_common_define.PROTO_TRACE_TIME_INFO):
                    trace_time_info = response.traceTimeInfo
            print "AcceptedMsgCount: %d" % acceptedMsgCount
        if type(responses) != type([]) and responses.HasField(swift_common_define.PROTO_TRACE_TIME_INFO):
            trace_time_info = responses.traceTimeInfo
        if trace_time_info:
            if trace_time_info.HasField(swift_common_define.PROTO_REQUEST_RECIVED_TIME):
                print "RequestRecivedTime: %s" % trace_time_info.requestRecivedTime
            if trace_time_info.HasField(swift_common_define.PROTO_RESPONSE_DONE_TIME):
                print "ResponseDoneTime: %s" % trace_time_info.responseDoneTime

        return "", "", 0
