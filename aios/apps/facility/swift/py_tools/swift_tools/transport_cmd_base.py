import re
import base_cmd
import swift_common_define
import swift_util
import swift_admin_delegate
import swift_broker_delegate
import swift.protocol.Common_pb2 as swift_proto_common


class TransportCmdBase(base_cmd.BaseCmd):
    def addOptions(self):
        super(TransportCmdBase, self).addOptions()

        # used by function test case
        self.parser.add_option('', '', '--brokeraddress',
                               action='store', dest="brokerAddress")

    def checkOptionsValidity(self, options):
        super(TransportCmdBase, self).checkOptionsValidity(options)

        # used by function test case
        if options.brokerAddress is not None:
            tmpStr = options.brokerAddress.strip()
            m = re.match("(.*):[0-9]+", tmpStr)
            if m is None:
                errMsg = "ERROR: broker address[%s] is invalid! "\
                         "Its format shold be ip:port" % tmpStr
                return False, errMsg
            ipStr = m.group(1)
            if not swift_util.SwiftUtil.checkIpAddress(ipStr):
                errMsg = "ERROR: broker ip address[%s] is invalid!" % ipStr
                return False, errMsg
        return True, ''

    def initMember(self, options):
        super(TransportCmdBase, self).initMember(options)

        # used by function test case
        self.brokerAddress = None
        if options.brokerAddress is not None:
            self.brokerAddress = options.brokerAddress.strip()

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)

        self.brokerDelegate = swift_broker_delegate.SwiftBrokerDelegate(self.options.username, self.options.passwd)
        return True

    def getBrokerAddressFromAdmin(self, topicName, partId):
        if self.brokerAddress:
            # just for function test case
            print "WARN: using user configured broker address[%s], "\
                "just for function test case." % self.brokerAddress
            return "tcp:" + self.brokerAddress

        ret, response, errMsg = self.adminDelegate.getTopicInfo(topicName)
        if not ret:
            print "ERROR: getTopicInfo for topic[%s] failed!" % topicName
            return None
        topicInfo = response.topicInfo
        brokerAddr = self.getBrokerAddress(topicInfo, partId)
        if not brokerAddr:
            print "ERROR: could not get broker address for partition[%d]" \
                % partId
            return None

        return brokerAddr

    def getBrokerAddress(self, topicInfo, partId):
        if len(topicInfo.partitionInfos) == 0:
            print "ERROR: there is no partition for topic[%s]" \
                % topicInfo.name
            return None

        brokerAddrMap = {}
        for partInfo in topicInfo.partitionInfos:
            if partInfo.id == partId or partId is None:
                if partInfo.HasField(swift_common_define.PROTO_STATUS):
                    if partInfo.status == swift_proto_common.PARTITION_STATUS_RUNNING and \
                            partInfo.HasField(swift_common_define.PROTO_BROKER_ADDRESS):
                        brokerAddrMap[partInfo.id] = "tcp:%s" % partInfo.brokerAddress
                    else:
                        print "ERROR: could not send messages to partition[%d], which current status[%s]." \
                            % (partInfo.id,
                               swift_util.SwiftUtil.protoEnumToString(partInfo, 'status')[17:])
                        if not brokerAddrMap.has_key(partId):
                            brokerAddrMap[partId] = None

        if partId is not None:
            if brokerAddrMap.has_key(partId):
                return brokerAddrMap[partId]
            else:
                return None
        for part, addr in brokerAddrMap.items():
            if addr is None:
                print 'ERROR: address for partition[%s] is None' % str(part)
                return None
        return brokerAddrMap
