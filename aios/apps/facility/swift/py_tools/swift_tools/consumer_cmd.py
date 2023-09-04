
import os
import swift_util
import swift_common_define
import transport_cmd_base
import swift.protocol.BrokerRequestResponse_pb2 as swift_proto_brokerrequestresponse
import swift.protocol.SwiftMessage_pb2 as swift_proto_swiftmessage
import swift.protocol.ErrCode_pb2 as swift_proto_errcode

class SwiftMsgDecoder(object):
    """
    return True, [(keyLen, key, valueLen, value, True|False)]
           False, []
    """
    def __init__(self):
        self.cursor = 0
        self.data = ""
        self.dataLen = 0

    def decode(self, data):
        self.cursor = 0
        self.data = data
        self.dataLen = len(data)
        conList = []
        while self.cursor < self.dataLen:
            ret, keyLen = self.__readLength()
            if not ret:
                return False, []
            ret, key = self.__readByte(keyLen)
            if not ret:
                return False, []
            ret, valLen = self.__readLength()
            if not ret:
                return False, []
            ret, val = self.__readByte(valLen)
            if not ret:
                return False, []
            ret, update = self.__readBool()
            if not ret:
                return False, []
            conList.append((keyLen, key, valLen, val, update))
        return True, conList

    def __readLength(self):
        val = 0
        for i in range(0,5):
            if self.cursor >= self.dataLen:
                return False, -1
            v = self.data[self.cursor:self.cursor+1]
            b = ord(v)
            self.cursor += 1
            offset = i * 7
            if i < 4:
                val = val | ((b&  0x7F) << offset)
            else:
                val = val | (b << offset)
            if (b & 0x80) == 0:
                return True, val
        return False, -1

    def __readByte(self, dataLen):
        if self.cursor + dataLen > self.dataLen:
            return False, ""
        else:
            data = self.data[self.cursor : self.cursor + dataLen]
            self.cursor += dataLen
            return True, data

    def __readBool(self):
        if self.cursor >= self.dataLen:
            return False, False
        ret, dataLen = self.__readLength()
        if not ret:
            return False, False

        if dataLen == 0:
            return True, False
        else:
            return True, True


class ConsumerGetMsgCmd(transport_cmd_base.TransportCmdBase):
    '''
    swift {gm|getmsg}
       {-z zookeeper_address  | --zookeeper=zookeeper_address}
       {-t topic_name         | --topic=topic_name}
       {-p partition_id       | --partid=partition_id}
       {-s msg_start_id       | --startid=msg_start_id}
       [-m msg_count          | --count=msg_count]
       [-f msg_file]          | --file=msg_file]
       [-y required_fileds]   | --required=required_fileds]
       [-o field_filter_desc] | --desc=field_filter_desc]

    options:
       -z zookeeper_address, --zookeeper=zookeeper_address   : required, zookeeper root address
       -t topic_name,        --topic=topic_name              : required, topic name, eg: news
       -p partition_id,      --partid=partition_id           : required, partition id in a topic
       -s msg_start_id       --startid=msg_start_id          : required, msg start id
       -m msg_count          --count=msg_count               : optional, msg count
       -r range_filter       --range=range_filter            : optional, range filter, default[0,65535]
       -x mask_filter        --mask=mask_filter              : optional, mask filter, default[0,0]
       -l max_total_size     --limit=max_total_size          : optional, max total size of response.
       -d decode_message     --decode=decode_message         : optional, decode field filter message, if specified,
       -f msg_file           --file=msg_file                 : optional, output, if specified, msg's data will be written to this file.
       -y required_fileds    --required=required_fields      : optional, required fields name. filter message field by required field name.
       -o field_filter_desc  --desc= field_filter_desc       : optional, filter message by field value.

Example:
    swift gm -z zfs://10.250.12.23:1234/root -t news -p 1 -s 1
    swift gm -z zfs://10.250.12.23:1234/root -t news -p 1 -s 1 -m 5
    swift gm -z zfs://10.250.12.23:1234/root -t news -p 1 -s 1 -m 5 -d true
    swift gm -z zfs://10.250.12.23:1234/root -t news -p 1 -s 1 -m 5 -f "/path/to/file"
    swift gm -z zfs://10.250.12.23:1234/root -t news -p 1 -s 1 -m 5 -r "1,3" -x "255,10" -f "/path/to/file"
    swift gm -z zfs://10.250.12.23:1234/root -t news -p 1 -s 1 -m 5 -n 'fieldA IN a|b|c AND fieldB IN 1|3'
    swift gm -z zfs://10.250.12.23:1234/root -t news -p 1 -s 1 -m 5 -y 'fieldA,fieldB'
    swift gm -z zfs://10.250.12.23:1234/root -t news -p 1 -s 1 -m 5 -o 'fieldA IN a|b'
    '''
    def addOptions(self):
        super(ConsumerGetMsgCmd, self).addOptions()
        self.parser.add_option('-t', '--topic', action='store', dest='topicName')
        self.parser.add_option('-p', '--partid', type='int', action='store', dest='partId')
        self.parser.add_option('-s', '--startid', type='int', action='store', dest='msgStartId')
        self.parser.add_option('-m', '--count', type='int', action='store', dest='msgCount')
        self.parser.add_option('-f', '--file', type='string', action='store', dest='msgFile')
        self.parser.add_option('-r', '--range', type='string', action='store', dest='range')
        self.parser.add_option('-x', '--mask', type='string', action='store', dest='mask')
        self.parser.add_option('-l', '--limit', type='int', action='store', dest='limit')
        self.parser.add_option('-d', '--decode', type='string', action='store', dest='decode')
        self.parser.add_option('-y', '--required', type='string', action='store', dest='required_fields')
        self.parser.add_option('-o', '--desc', type='string', action='store', dest='desc_fields')

    def initMember(self, options):
        super(ConsumerGetMsgCmd, self).initMember(options)
        if options.topicName != None:
            self.topicName = options.topicName

        if options.partId != None:
            self.partId = options.partId

        if options.msgStartId != None:
            self.msgStartId = options.msgStartId

        self.msgCount = None
        if options.msgCount != None:
            self.msgCount = options.msgCount

        self.msgFile = None
        if options.msgFile != None:
            self.msgFile = options.msgFile

        self.range = None
        if options.range != None:
            self.range = options.range

        self.mask = None
        if options.mask != None:
            self.mask = options.mask

        self.decode = False
        if options.decode != None and options.decode != 'false':
            self.decode = True

        self.maxTotalSize = None
        if options.limit != None:
            self.maxTotalSize = options.limit

        self.requiredField = None
        if options.required_fields != None:
            self.requiredField = options.required_fields

        self.fieldFilterDesc = None
        if options.desc_fields != None:
            self.fieldFilterDesc = options.desc_fields

    def checkOptionsValidity(self, options):
        ret, errMsg = super(ConsumerGetMsgCmd, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg

        if options.topicName == None:
            errMsg = "ERROR: topic must be specified!"
            return False, errMsg

        if options.partId == None:
            errMsg = "ERROR: partid must be specified!"
            return False, errMsg

        if options.msgStartId == None:
            errMsg = "ERROR: startid must be specified!"
            return False, errMsg

        if options.msgFile != None:
            if os.path.exists(options.msgFile) \
                    and os.path.isdir(options.msgFile):
                errMsg = "ERROR: msg_file[%s] is a directory!" % options.msgFile
                return False, errMsg

        if options.limit and options.limit < 0:
            errMsg = "ERROR: invalid limit : %d" % options.limit
            return False, errMsg
        return True, ''

    def run(self):
        response = swift_proto_errcode.CommonResponse()
        response.errorInfo.errCode = swift_proto_errcode.ERROR_NONE
        brokerAddr = self.getBrokerAddressFromAdmin(self.topicName, self.partId)
        if not brokerAddr:
            response.errorInfo.errCode = swift_proto_errcode.ERROR_UNKNOWN
            response.errorInfo.errMsg =  "ERROR: getBrokerAddressFromAdmin for topic[%s], "\
                                         "partition id[%d] faild!" % (self.topicName, self.partId)
            print response.errorInfo.errMsg
            return "", response, 1

        ret, swiftFilter = self.constructFilter(self.range, self.mask)
        if not ret:
            response.errorInfo.errCode = swift_proto_errcode.ERROR_UNKNOWN
            response.errorInfo.errMsg =  "ERROR: construct filter error for topic[%s]" % self.topicName
            return "", response, 1

        ret, data = self.brokerDelegate.getMessage(brokerAddr,
                                                   self.topicName,
                                                   self.partId,
                                                   self.msgStartId,
                                                   self.msgCount,
                                                   swiftFilter,
                                                   self.maxTotalSize,
                                                   self.requiredField,
                                                   self.fieldFilterDesc)
        if not ret:
            response.errorInfo.errCode = swift_proto_errcode.ERROR_UNKNOWN
            print "ERROR: get message failed!"
            print data
            response.errorInfo.errMsg = data
            return ret, response, 1
        print "Get message success!"
        if self.fromApi:
            return ret, data, 0

        response = data
        if response.HasField(swift_common_define.PROTO_MAX_MSG_ID):
            print "MaxMsgId: %d" % response.maxMsgId
        if response.HasField(swift_common_define.PROTO_MAX_TIME_STAMP):
            timeStr = swift_util.SwiftUtil.convertTime2Str(response.maxTimestamp/1000000,
                                                           '%Y/%m/%d %H:%M:%S')
            print "MaxTimestamp: %s" % timeStr
            print "MaxTimestamp_us: %s" % response.maxTimestamp

        if response.HasField(swift_common_define.PROTO_NEXT_MSG_ID):
            print "NextMsgId: %d" % response.nextMsgId
        if response.HasField(swift_common_define.PROTO_NEXT_TIME_STAMP):
            timeStr = swift_util.SwiftUtil.convertTime2Str(response.nextTimestamp/1000000,
                                                           '%Y/%m/%d %H:%M:%S')
            print "NextTimestamp: %s" % timeStr
            print "NextTimestamp_us: %s" % response.nextTimestamp
        if response.HasField(swift_common_define.PROTO_TRACE_TIME_INFO):
            trace_time_info = response.traceTimeInfo
            if trace_time_info.HasField(swift_common_define.PROTO_REQUEST_RECIVED_TIME):
                print "RequestRecivedTime: %s" % trace_time_info.requestRecivedTime
            if trace_time_info.HasField(swift_common_define.PROTO_RESPONSE_DONE_TIME):
                print "ResponseDoneTime: %s" % trace_time_info.responseDoneTime

        if len(response.msgs) > 0:
            if self.msgFile:
                # msg will be written to file
                print "WARN: msg data from partition[%d], broker[%s] is written "\
                    "to file[%s]" % (self.partId, brokerAddr, self.msgFile)
                ret = self.writeMsgs2File(response.msgs)
                if not ret:
                    print "ERROR: write msg's data to file[%s] failed!" \
                        % self.msgFile
                    return "", "", 1
            else:
                print "\nMessages from partition[%d], broker[%s]:" \
                    % (self.partId, brokerAddr)
                self.printMsgs(response.msgs)
        return "", "", 0

    def constructFilter(self, rangeFilter, maskFilter):
        swiftFilter = swift_proto_swiftmessage.Filter()
        if rangeFilter is not None:
            range = rangeFilter.split(',')
            if len(range) != 2:
                print "ERROR: invalid filter range[%s]" % rangeFilter
                return False, swift_proto_swiftmessage.Filter()
            try:
                swiftFilter.__setattr__('from', int(range[0]))
                swiftFilter.to = int(range[1])
            except Exception, e:
                print "ERROR: invalid filter range[%s]" % options.range
                return False, swift_proto_swiftmessage.Filter()

        if maskFilter is not None:
            mask = maskFilter.split(',')
            if len(mask) != 2:
                print "ERROR: invalid filter mask[%s]" % maskFilter
                return False, swift_proto_swiftmessage.Filter()
            try:
                swiftFilter.uint8FilterMask = int(mask[0])
                swiftFilter.uint8MaskResult = int(mask[1])
            except Exception, e:
                print "ERROR: invalid filter mask[%s]" % options.mask
                return False, swift_proto_swiftmessage.Filter()
        return True, swiftFilter


    def writeMsgs2File(self, msgs):
        if os.path.exists(self.msgFile):
            os.remove(self.msgFile)

        dirName = os.path.dirname(self.msgFile)
        if not os.path.exists(dirName):
            os.makedirs(dirName)

        ret = False
        try:
            try:
                f = open(self.msgFile, "a+")  #append mode
                for msg in msgs:
                    if msg.HasField("data"):
                        f.write(msg.data)
                ret = True
            except Exception, e:
                print str(e)
        finally:
            try:
                f.close()
            except Exception, e:
                print str(e)
        return ret

    def printMsgs(self, msgs):
        for msg in msgs:
            print "-------------------------------"
            print "MsgId: %d" % msg.msgId
            timeStr = swift_util.SwiftUtil.convertTime2Str(msg.timestamp/1000000,
                                                           '%Y/%m/%d %H:%M:%S')
            print "Timestamp: %s" % timeStr
            print "Timestamp_us: %s" % msg.timestamp
            print "uint16Payload: %d" % msg.uint16Payload
            print "uint8MaskPayload: %d" % msg.uint8MaskPayload
            if self.decode:
                decoder = SwiftMsgDecoder()
                ret, val = decoder.decode(msg.data)
                if ret:
                    print "Data:[%s]" % str(val)
                else:
                    print "decode msg failed!"
                    print "Data: [%s]" % msg.data
            else:
                print "Data: [%s]" % msg.data

class ConsumerGetMaxMsgIdCmd(transport_cmd_base.TransportCmdBase):
    '''
    swift {getmaxmsgid|gmi}
       {-z zookeeper_address  | --zookeeper=zookeeper_address}
       {-t topic_name         | --topic=topic_name}
       {-p partition_id       | --partid=partition_id}
       {-b broker_address     | --broker=broker_address}

    options:
       -z zookeeper_address, --zookeeper=zookeeper_address   : required, zookeeper root address
       -t topic_name,        --topic=topic_name              : required, topic name, eg: news
       -p partition_id,      --partid=partition_id           : required, partition id in a topic
       -b broker_address,    --broker                        : optional, optimize need

Example:
    swift gmi -z zfs://10.250.12.23:1234/root -t news -p 1
    '''
    def addOptions(self):
        super(ConsumerGetMaxMsgIdCmd, self).addOptions()
        self.parser.add_option('-t', '--topic', action='store', dest='topicName')
        self.parser.add_option('-p', '--partid', type='int', action='store', dest='partId')
        self.parser.add_option('-b', '--broker', type='string', action='store', dest='broker')

    def initMember(self, options):
        super(ConsumerGetMaxMsgIdCmd, self).initMember(options)
        if options.topicName != None:
            self.topicName = options.topicName
        if options.partId != None:
            self.partId = options.partId
        self.broker = options.broker

    def checkOptionsValidity(self, options):
        ret, errMsg = super(ConsumerGetMaxMsgIdCmd, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg

        if options.topicName == None:
            errMsg = "ERROR: topic must be specified!"
            return False, errMsg

        if options.partId == None:
            errMsg = "ERROR: partid must be specified!"
            return False, errMsg

        return True, ''

    def run(self):
        commonError = swift_proto_errcode.CommonResponse()
        error = commonError.errorInfo
        error.errCode = swift_proto_errcode.ERROR_NONE
        brokerAddr = self.broker
        if brokerAddr is None:
            brokerAddr = self.getBrokerAddressFromAdmin(self.topicName, self.partId)
            if not brokerAddr:
                error.errCode = swift_proto_errcode.ERROR_UNKNOWN
                error.errMsg = "ERROR: getBrokerAddressFromAdmin for topic[%s], "\
                    "partition id[%d] faild!" % (self.topicName, self.partId)
                print error.errMsg
                return "", commonError, 1

        ret, data = self.brokerDelegate.getMaxMessageId(brokerAddr,
                                                        self.topicName,
                                                        self.partId)
        if not ret:
            error.errCode = swift_proto_errcode.ERROR_UNKNOWN
            error.errMsg = data
            print "ERROR: get max message id failed!"
            print data
            return "", commonError, 1
        if self.fromApi:
            return ret, data, 0
        print "Get max message id success!"
        response = data
        if response.HasField(swift_common_define.PROTO_MSG_ID):
            print "MsgId: %d" % response.msgId
        if response.HasField(swift_common_define.PROTO_TIME_STAMP):
            ts = response.timestamp
            # timeStr = swift_util.SwiftUtil.convertTime2Str(response.timestamp/1000000,
            #                                                '%Y/%m/%d %H:%M:%S')
            print "Timestamp: %ld" % ts
        return "", "", 0

class ConsumerGetMinMsgIdByTime(transport_cmd_base.TransportCmdBase):
    '''
    swift {getminmsgidbytime|gmit}
       {-z zookeeper_address  | --zookeeper=zookeeper_address}
       {-t topic_name         | --topic=topic_name}
       {-p partition_id       | --partid=partition_id}
       {-s time_str           | --time=time_str}

    options:
       -z zookeeper_address, --zookeeper=zookeeper_address   : required, zookeeper root address
       -t topic_name,        --topic=topic_name              : required, topic name, eg: news
       -p partition_id,      --partid=partition_id           : required, partition id in a topic
       -s time_str,          --time=time_str                 : optional, format "2012/02/23 18:17:51"
       -u timestamp_us,      --timestamp_us=timestamp_us     : optional, format "1505347277021798", us

Example:
    swift gmit -z zfs://10.250.12.23:1234/root -t news -p 1 -s "2012/02/23 18:17:51"
    swift gmit -z zfs://10.250.12.23:1234/root -t news -p 1 -u 1505347277021798
    '''
    def addOptions(self):
        super(ConsumerGetMinMsgIdByTime, self).addOptions()
        self.parser.add_option('-t', '--topic', action='store', dest='topicName')
        self.parser.add_option('-p', '--partid', type='int', action='store', dest='partId')
        self.parser.add_option('-s', '--timestamp', action='store', dest='timestampStr')
        self.parser.add_option('-u', '--timestamp_us', type='long', action='store', dest='timestampUs')

    def initMember(self, options):
        super(ConsumerGetMinMsgIdByTime, self).initMember(options)
        if options.topicName != None:
            self.topicName = options.topicName

        if options.partId != None:
            self.partId = options.partId

        self.timestampStr = None
        if options.timestampStr != None:
            self.timestampStr = options.timestampStr
        self.timestampUs = None
        if options.timestampUs != None:
            self.timestampUs = options.timestampUs

    def checkOptionsValidity(self, options):
        ret, errMsg = super(ConsumerGetMinMsgIdByTime, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg

        if options.topicName == None:
            errMsg = "ERROR: topic must be specified!"
            return False, errMsg

        if options.partId == None:
            errMsg = "ERROR: partid must be specified!"
            return False, errMsg

        if options.timestampStr == None and options.timestampUs == None:
            errMsg = "ERROR: time must be specified!"
            return False, errMsg

        return True, ''

    def run(self):
        error = swift_proto_errcode.ErrorInfo()
        error.errCode = swift_proto_errcode.ERROR_UNKNOWN
        brokerAddr = self.getBrokerAddressFromAdmin(self.topicName,
                                                    self.partId)
        if not brokerAddr:
            error.errMsg = "ERROR: getBrokerAddressFromAdmin for topic[%s], "\
                           "partition id[%d] faild!" % (self.topicName, self.partId)
            print error.errMsg
            return "", error, 1

        timestamp = self.timestampUs
        if self.timestampStr:
            ret, timestamp, error.errMsg = self.getTimeStamp(self.timestampStr)
            if not ret:
                return ret, error, 1

            timestamp = timestamp * 1000000 # us
        ret, data = self.brokerDelegate.getMinMessageIdByTime(brokerAddr,
                                                              self.topicName,
                                                              self.partId,
                                                              timestamp)
        if not ret:
            print "ERROR: get min msg id by time failed!"
            print data
            error.errMsg = data
            return "", error, 1
        print "Get min msg id by time success!"
        if self.fromApi:
            error.errCode = swift_proto_errcode.ERROR_NONE
            return ret, data, 0
        response = data
        if response.HasField(swift_common_define.PROTO_MSG_ID):
            print "MsgId: %d" % response.msgId
        if response.HasField(swift_common_define.PROTO_TIME_STAMP):
            timeStr = swift_util.SwiftUtil.convertTime2Str(response.timestamp/1000000,
                                                           '%Y/%m/%d %H:%M:%S')
            print "Timestamp: %s" % timeStr
        return "", "", 0

    def getTimeStamp(self, timeStr):
        timestamp = 0
        try:
            timestamp = swift_util.SwiftUtil.convertStr2Time(timeStr,
                                                             "%Y/%m/%d %H:%M:%S")
        except Exception, e:
            error = "ERROR: change timestamp string [%s] to time failed! " % timeStr
            error +=  str(e)
            print error
            return False, None, error

        return True, timestamp, ''  # sec
