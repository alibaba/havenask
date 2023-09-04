#!/bin/env python
import swift_util
import base_cmd
import time
import swift_common_define
import swift_admin_delegate
import status_analyzer
import swift.protocol.AdminRequestResponse_pb2 as swift_proto_admin
import swift.protocol.Common_pb2 as swift_proto_common
import swift.protocol.ErrCode_pb2 as swift_proto_errcode


class SwiftStatusCmd(base_cmd.BaseCmd):
    '''
    swift {gs|getstatus}
       {-z zookeeper_address  | --zookeeper=zookeeper_address}
       [-c cmd_type           | --cmdtype=cmd_type]
       [-m                    | --message]

    options:
       -z zookeeper_address, --zookeeper=zookeeper_address   : required, zookeeper root address
       -c cmd_type,          --cmdtype=cmd_type              : optional, command type, default worker
          worker:            worker status
             -r role_type,   --role=role_type                : optional, admin or broker; default is all
          role:              role address and status information
             -r role_type,   --role=role_type                : optional, admin or broker; default is all
             -s role_status  --status=role_status            : optional, alive, dead, both; default is both
          adminleader:       admin leader information
             -a              --leaderhistory                 : optional, show admin leader history
          error:             error information about worker and partition
             -e error_type   --error=error_type              : optional, worker or partition,
                                                               default is both
             -t error_time,  --time=error_time               : optional, format "2012/02/23 18:17:51"
             -n error_count  --num=error_count               : optional
                                                               one of error_time and error_count
                                                               must be specified, if both of them are specified,
                                                               use error_time
             -l error_level  --level=error_level             : optional, error level(info/fatal(default))
       -m,                   --message                       : output format is protocol message

    example:
       ./swift gs -z zfs://10.250.12.23:1234/root

       ./swift gs -z zfs://10.250.12.23:1234/root -c worker
       ./swift gs -z zfs://10.250.12.23:1234/root -c worker -r admin
       ./swift gs -z zfs://10.250.12.23:1234/root -c worker -m

       ./swift gs -z zfs://10.250.12.23:1234/root -c role
       ./swift gs -z zfs://10.250.12.23:1234/root -c role -r admin -s dead

       ./swift gs -z zfs://10.250.12.23:1234/root -c adminleader
       ./swift gs -z zfs://10.250.12.23:1234/root -c adminleader --leaderhistory

       ./swift gs -z zfs://10.250.12.23:1234/root -c error -t "2012/02/23 18:17:51"
       ./swift gs -z zfs://10.250.12.23:1234/root -c error -n 3 -l info
       ./swift gs -z zfs://10.250.12.23:1234/root -c error -e worker
    '''

    def __init__(self, fromApi=False):
        super(SwiftStatusCmd, self).__init__(fromApi)
        self.str2RoleStatusMap = {"both": swift_proto_admin.ROLE_STATUS_ALL,
                                  "alive": swift_proto_admin.ROLE_STATUS_LIVING,
                                  "dead": swift_proto_admin.ROLE_STATUS_DEAD,
                                  }

    def addOptions(self):
        super(SwiftStatusCmd, self).addOptions()
        self.parser.add_option('-c', '--cmdtype', type='choice',
                               action='store', dest='cmdType',
                               choices=['worker', 'role',
                                        'error', 'adminleader'],
                               default='worker')

        # for role status and address
        self.parser.add_option('-r', '--role', type="choice",
                               action='store', dest='roleType',
                               choices=['admin', 'broker', 'all'],
                               default='all')
        self.parser.add_option('-s', '--status', type="choice",
                               action='store', dest='roleStatus',
                               choices=['alive', 'dead', 'both'],
                               default='both')

        # for error
        self.parser.add_option('-e', '--error', type='choice',
                               action='store', dest='errorType',
                               choices=['worker', 'partition', 'both'],
                               default="both")
        self.parser.add_option('-t', '--time', type='string',
                               action='store', dest='errorTime')
        self.parser.add_option('-n', '--num', type='int',
                               action='store', dest='errorCount')
        self.parser.add_option('-l', '--level', type='choice',
                               action='store', dest='errorLevel',
                               choices=['info', 'fatal'],
                               default='fatal')

        # for leader
        self.parser.add_option('-a', '--leaderhistory',
                               action='store_true',
                               dest='leaderHistory',
                               default=False)

        # for proto message
        self.parser.add_option('-m', '--message',
                               action='store_true',
                               dest='protoMsg',
                               default=False)

    def initMember(self, options):
        super(SwiftStatusCmd, self).initMember(options)
        self.cmdType = options.cmdType

        self.roleType = options.roleType
        self.roleStatus = options.roleStatus

        self.errorType = options.errorType

        self.errorTime = None
        if options.errorTime is not None:
            self.errorTime = options.errorTime

        self.errorCount = None
        if options.errorCount is not None:
            self.errorCount = options.errorCount

        self.errorLevel = None
        if options.errorLevel is not None:
            self.errorLevel = self.errorLevelStr2Enum(options.errorLevel)

        self.protoMsg = options.protoMsg
        self.leaderHistory = options.leaderHistory

    def errorLevelStr2Enum(self, string):
        errorLevelDict = {"info": swift_proto_errcode.ERROR_LEVEL_INFO,
                          "fatal": swift_proto_errcode.ERROR_LEVEL_FATAL,
                          }
        return errorLevelDict[string]

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)

        return True

    def run(self):
        ret = True
        response = {}
        errMsg = None
        if self.cmdType == "adminleader":
            if self.leaderHistory:
                response = self.printAdminLeaderHistory()
            else:
                response = self.printAdminLeader()
        else:
            if self.cmdType == "worker":
                ret, response, errMsg = self.printWorkerStatus()
            elif self.cmdType == "error":
                ret, response, errMsg = self.printError()
            elif self.cmdType == "role":
                ret, response, errMsg = self.printRoleAdress()
            else:
                print "ERROR: get swift status failed!"
                print "ERROR: unsupported cmd type[%s]" % self.cmdType
                return "", "", 1

        if self.fromApi:
            return ret, response, errMsg

        if not ret:
            return "", "", 1

        return "", response, 0

    def printWorkerStatus(self):
        ret, response, errMsg = self.adminDelegate.getAllWorkerStatus()
        if self.fromApi:
            return ret, response, errMsg
        if not ret:
            print errMsg
            return False, response, errMsg
        workersStatus = response.workers
        if len(workersStatus) == 0:
            print "There is no worker in swift."
            return True, response, errMsg

        analyzer = status_analyzer.WorkerStatusAnalyzer()

        if self.roleType == "admin":
            workersStatus = analyzer.getAdminWorkerStatus(workersStatus)
        elif self.roleType == "broker":
            workersStatus = analyzer.getBrokerWorkerStatus(workersStatus)
        else:
            workersStatus = analyzer.getSortedWorkerStatus(workersStatus)

        if self.protoMsg:
            # output format is protocol message
            for status in workersStatus:
                print status
        else:
            # formated output
            self.printFormatedWorkerStatus(workersStatus)

        return True, response, errMsg

    def printFormatedWorkerStatus(self, workersStatus):
        for workerStatus in workersStatus:
            if workerStatus.current.role == swift_proto_common.ROLE_TYPE_ADMIN:
                self.printAdminWorkerStatus(workerStatus)
            elif workerStatus.current.role == swift_proto_common.ROLE_TYPE_BROKER:
                self.printBrokerWorkerStatus(workerStatus)
            else:
                pass

    def printAdminWorkerStatus(self, workerStatus):
        heartbeat = workerStatus.current
        role = swift_util.SwiftUtil.protoEnumToString(heartbeat, 'role')[10:]
        status = ""
        if heartbeat.alive:
            status = "ALIVE"
        else:
            status = "DEAD"

        decisionStatus = ""
        if workerStatus.HasField("decisionStatus"):
            decisionStatus = swift_util.SwiftUtil.protoEnumToString(workerStatus,
                                                                    "decisionStatus")[8:]
        statusStr = "Role[%(role_type)s].Address[%(address)s]."\
            "Status[%(status)s].IsValid[%(is_valid)s]."\
            "DecisionStatus[%(desision_status)s]." \
            "IsStrange[%(is_strange)s]" % \
            {"role_type": role,
             "status": status,
             "address": heartbeat.address,
             "is_valid": (not workerStatus.isInvalid),
             "desision_status": decisionStatus,
             "is_strange": (workerStatus.isStrange)
             }
        print statusStr

    def printBrokerWorkerStatus(self, workerStatus):
        heartbeat = workerStatus.current
        role = swift_util.SwiftUtil.protoEnumToString(heartbeat, 'role')[10:]
        status = ""
        if heartbeat.alive:
            status = "ALIVE"
        else:
            status = "DEAD"

        decisionStatus = ""
        if workerStatus.HasField("decisionStatus"):
            decisionStatus = swift_util.SwiftUtil.protoEnumToString(workerStatus,
                                                                    "decisionStatus")[8:]
        statusStr = "Role[%(role_type)s].Address[%(address)s]."\
            "Status[%(status)s].IsValid[%(is_valid)s]."\
            "DecisionStatus[%(desision_status)s]."\
            "FreeResource[%(free_resource)d].MaxResource[%(max_resource)d]."\
            "IsStrange[%(is_strange)s]" %\
            {"role_type": role,
             "status": status,
             "address": heartbeat.address,
             "is_valid": (not workerStatus.isInvalid),
             "desision_status": decisionStatus,
             "free_resource": workerStatus.freeResource,
             "max_resource": workerStatus.maxResource,
             "is_strange": workerStatus.isStrange
             }
        print statusStr

        if len(heartbeat.tasks) > 0:
            for taskStatus in heartbeat.tasks:
                topicName = taskStatus.taskInfo.partitionId.topicName
                partStatus = swift_util.SwiftUtil.protoEnumToString(taskStatus,
                                                                    'status')[17:]
                tmpStr = "%d:%s" % (taskStatus.taskInfo.partitionId.id,
                                    partStatus)
                curPartStatus = "Role[%(role_type)s].Address[%(address)s]."\
                    "Current.Topic[%(topic_name)s].Partition[%(part_status)s]" %\
                    {"role_type": role,
                     "address": heartbeat.address,
                     "topic_name": topicName,
                     "part_status": tmpStr,
                     }
                print curPartStatus

        if len(workerStatus.target) > 0:
            targets = workerStatus.target
            for target in targets:
                tarPartStatus = "Role[%(role_type)s].Address[%(address)s]."\
                    "Target.Topic[%(topic_name)s].Partition[%(part_ids)s]" %\
                    {"role_type": role,
                     "address": heartbeat.address,
                     "topic_name": target.topicName,
                     "part_ids": target.id,
                     }
                print tarPartStatus

    def printRoleAdress(self):
        print 'begin:', time.time()
        ret = True
        response = None
        errMsg = None
        if self.roleType == "admin":
            ret, response, errMsg = self.getAndPrintAdminMachineList()
        elif self.roleType == "broker":
            ret, response, errMsg = self.getAndPrintBrokerMachineList()
        elif self.roleType == "all":
            ret, response, errMsg = self.getAndPrintAdminMachineList()
            print 'after admin:', time.time()
            ret2, response2, errMsg2 = self.getAndPrintBrokerMachineList()
            response = [response, response2]
            ret = ret and ret2
        else:
            print "ERROR: not support roleType[%s]" % self.roleType
        print 'end:', time.time()
        return ret, response, errMsg

    def getAndPrintAdminMachineList(self):
        ret, response, errMsg = self.adminDelegate.getRoleAddress(
            swift_proto_common.ROLE_TYPE_ADMIN,
            self.str2RoleStatusMap[self.roleStatus])
        if self.fromApi:
            return ret, response, errMsg
        if not ret:
            print errMsg
            return False, response, errMsg

        addressGroupList = response.addressGroup
        if len(addressGroupList) == 0:
            return True, response, errMsg

        if self.protoMsg:
            # output format is protocol message
            for address in addressGroupList:
                print address
        else:
            aliveList, deadList = self.getMachineList(addressGroupList,
                                                      swift_proto_common.ROLE_TYPE_ADMIN)
            if len(aliveList) > 0:
                print "Admin(ALIVE) Machine List:"
                for ip in aliveList:
                    print "%25s" % ip

            if len(deadList) > 0:
                print "Admin(DEAD) Machine List:"
                for ip in deadList:
                    print "%25s" % ip

        return True, response, errMsg

    def getMachineList(self, addressGroupList, roleType):
        aliveList = []
        deadList = []
        for address in addressGroupList:
            if address.role == roleType and \
               address.status == swift_proto_admin.ROLE_STATUS_LIVING:
                aliveList.extend(address.addressList)
            elif address.role == roleType and \
                    address.status == swift_proto_admin.ROLE_STATUS_DEAD:
                deadList.extend(address.addressList)
        return aliveList, deadList

    def getAndPrintBrokerMachineList(self):
        ret, response, errMsg = self.adminDelegate.getRoleAddress(
            swift_proto_common.ROLE_TYPE_BROKER,
            self.str2RoleStatusMap[self.roleStatus])
        if self.fromApi:
            return ret, response, errMsg
        if not ret:
            print "Get broker machine list and status failed!"
            print errMsg
            return False, response, errMsg

        addressGroupList = response.addressGroup
        if len(addressGroupList) == 0:
            return True, response, errMsg

        if self.protoMsg:
            # output format is protocol message
            for address in addressGroupList:
                print address
        else:
            aliveList, deadList = self.getMachineList(addressGroupList,
                                                      swift_proto_common.ROLE_TYPE_BROKER)
            if len(aliveList) > 0:
                print "Broker(ALIVE) Machine List:"
                for ip in aliveList:
                    print "%25s" % ip

            if len(deadList) > 0:
                print "Broker(DEAD) Machine List:"
                for ip in deadList:
                    print "%25s" % ip

        return True, response, errMsg

    def printError(self):
        self.errorTimeInt = None
        if self.errorTime is not None:
            try:
                self.errorTimeInt = swift_util.SwiftUtil.convertStr2Time(self.errorTime,
                                                                         "%Y/%m/%d %H:%M:%S")
            except Exception as e:
                print "ERROR: change time str[%s] to int failed!" \
                    % self.errorTime
                print str(e)
            return False

        ret = True
        errMsg = None
        if self.errorType == "worker":
            ret, response, errMsg = self.printWorkerError()
        elif self.errorType == "partition":
            ret, response, errMsg = self.printPartitionError()
        else:
            ret, response, errMsg = self.printWorkerError()
            print ""
            ret2, response2, errMsg2 = self.printPartitionError()
            response = [response, response2]
            ret = ret and ret2
            if errMsg is None:
                errMsg = errMsg2
            else:
                errMsg += errMsg2

        return ret, response, errMsg

    def printWorkerError(self):
        ret, response, errMsg = self.adminDelegate.getWorkerError(
            self.errorTimeInt, self.errorCount, self.errorLevel)
        if self.fromApi:
            return ret, response, errMsg
        if not ret:
            print "ERROR: get worker error failed!"
            print data
            return False, response, errMsg

        workerErrors = response.errors
        if len(workerErrors) == 0:
            return True, response, errMsg

        if self.protoMsg:
            # output format is protocol message
            for error in workerErrors:
                print error
        else:
            self.printFormatedWorkerError(workerErrors)

        return True, response, errMsg

    def printFormatedWorkerError(self, workerErrors):
        print "Worker Error:"
        for error in workerErrors:
            role = swift_util.SwiftUtil.protoEnumToString(error,
                                                          'role')[10:]
            timeStr = ""
            if error.errTime > 0:
                timeStr = swift_util.SwiftUtil.convertTime2Str(error.errTime / (1000 * 1000),
                                                               "%Y/%m/%d %H:%M:%S")

            errorStr = "Role[%(role_type)s].Address[%(address)s]."\
                "ErrorCode[%(err_code)d].ErrorTime[%(err_time)s]."\
                "ErrorMsg[%(err_msg)s]" % \
                {"role_type": role,
                 "address": error.workerAddress,
                 "err_code": error.errCode,
                 "err_time": timeStr,
                 "err_msg": error.errMsg,
                 }
            print errorStr

    def printPartitionError(self):
        ret, response, errMsg = self.adminDelegate.getPartitionError(
            self.errorTimeInt, self.errorCount, self.errorLevel)
        if self.fromApi:
            return ret, response, errMsg
        if not ret:
            print "ERROR: get partition error failed!"
            print errMsg
            return False, response, errMsg

        partErrors = response.errors

        if len(partErrors) == 0:
            return True, response, errMsg

        if self.protoMsg:
            # output format is protocol message
            for error in partErrors:
                print error
        else:
            self.printFormatedPartError(partErrors)

        return True, response, errMsg

    def printFormatedPartError(self, partErrors):
        print "Partition Error:"
        for error in partErrors:
            timeStr = ""
            if error.errTime > 0:
                timeStr = swift_util.SwiftUtil.convertTime2Str(error.errTime / (1000 * 1000),
                                                               "%Y/%m/%d %H:%M:%S")

            errorStr = "Topic[%(topic_name)s].Partition[%(part_id)d]."\
                "ErrorCode[%(err_code)d].ErrorTime[%(err_time)s]."\
                "ErrorMsg[%(err_msg)s]" % \
                {"topic_name": error.partitionId.topicName,
                 "part_id": error.partitionId.id,
                 "err_code": error.errCode,
                 "err_time": timeStr,
                 "err_msg": error.errMsg,
                 }
            print errorStr

    def printAdminLeader(self):
        adminLeaderInfo = self.adminDelegate.getAdminLeaderInfo()
        if self.fromApi:
            return adminLeaderInfo
        if not adminLeaderInfo:
            print "ERROR: could not get curren admin leader information! "\
                "Please make sure swift system has been started."
            return False
        if adminLeaderInfo.HasField(swift_common_define.PROTO_ADDRESS):
            adminSpec = adminLeaderInfo.address
        if adminLeaderInfo.HasField(swift_common_define.PROTO_SYS_STOP):
            adminStopped = adminLeaderInfo.sysStop
        if (not adminStopped) and adminSpec:
            if self.protoMsg:
                # output format is protocol message
                print adminLeaderInfo
            else:
                timeStr = ""
                if adminLeaderInfo.startTimeSec > 0:
                    timeStr = swift_util.SwiftUtil.convertTime2Str(adminLeaderInfo.startTimeSec,
                                                                   "%Y/%m/%d %H:%M:%S")
                leaderInfo = "AdminLeader.Address[%(address)s]."\
                    "StartTime[%(start_time)s]" % \
                    {"address": adminLeaderInfo.address,
                     "start_time": timeStr,
                     }
                print leaderInfo
        else:
            print "ERROR: could not get current admin leader information! "\
                "Please make sure swift system has been started."
            return False
        return True

    def printAdminLeaderHistory(self):
        adminLeaderHis = self.adminDelegate.getAdminLeaderHistory()
        if self.fromApi:
            return adminLeaderHis
        if not adminLeaderHis:
            print "ERROR: get admin leader history failed! " \
                "There is no admin leader history."
            return False

        if len(adminLeaderHis.leaderInfos) == 0:
            print "There is no admin leader history. " \
                "Please make sure swift system has been started."
            return True

        for leader in adminLeaderHis.leaderInfos:
            if self.protoMsg:
                # output format is protocol message
                print leader
            else:
                timeStr = ""
                if leader.startTimeSec > 0:
                    timeStr = swift_util.SwiftUtil.convertTime2Str(leader.startTimeSec,
                                                                   "%Y/%m/%d %H:%M:%S")
                leaderInfo = "AdminLeader.Address[%(address)s]."\
                    "StartTime[%(start_time)s]" % \
                    {"address": leader.address,
                     "start_time": timeStr,
                     }
                print leaderInfo

        return True
