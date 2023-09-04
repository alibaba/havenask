# -*- coding: utf-8 -*-
import os
import json
from base_cmd import BaseCmd
from tools_config import ToolsConfig
from process import Process
import swift_common_define
import swift_admin_delegate
import swift.protocol.ErrCode_pb2 as swift_proto_errcode


class AppBaseCmd(BaseCmd):
    def __init__(self, fromApi=False):
        super(AppBaseCmd, self).__init__(fromApi)

    def addOptions(self):
        super(AppBaseCmd, self).addOptions()

    def initMember(self, options):
        super(AppBaseCmd, self).initMember(options)

    def parseConfigInfo(self, configDir):
        swiftConf = configDir + '/swift.conf'
        if self.fileUtil.exists(swiftConf):
            content = self.fileUtil.cat(swiftConf)
            import ConfigParser
            import StringIO
            buf = StringIO.StringIO(content)
            config = ConfigParser.ConfigParser()
            config.readfp(buf)
            self.userName = config.get('common', 'user_name')
            self.serviceName = config.get('common', 'service_name')
            self.hippoRoot = config.get('common', 'hippo_root')
            self.adminCount = config.get('common', 'admin_count')
            self.brokerCount = config.get('common', 'broker_count')
            self.zkRoot = config.get('common', 'zookeeper_root')
            self.appName = self.userName + '_' + self.serviceName
            return True
        return False

    def getAppInfo(self):
        from hippo_py_sdk.hippo_client import HippoClient
        hippoClient = HippoClient(self.hippoRoot)
        response = hippoClient.get_app_status(self.appName)
        return response

    def parseWorkerCount(self, response):
        from hippo.proto.Common_pb2 import ProcessStatus
        adminRunningCount = 0
        brokerRunningCount = 0
        brokerCount = 0
        adminCount = 0
        for lastAllocateResponse in response.lastAllocateResponse:
            if "__internal_appmaster_resource_tag__" == lastAllocateResponse.resourceTag:
                adminCount = len(lastAllocateResponse.assignedSlots)
                for assignedSlot in lastAllocateResponse.assignedSlots:
                    if len(assignedSlot.processStatus) != 0 and\
                            assignedSlot.processStatus[0].status == ProcessStatus.PS_RUNNING:
                        adminRunningCount += 1
            else:
                brokerCount += 1
                assignedSlot = lastAllocateResponse.assignedSlots[0]
                if len(assignedSlot.processStatus) != 0 and \
                        assignedSlot.processStatus[0].status == ProcessStatus.PS_RUNNING:
                    brokerRunningCount += 1
        return adminRunningCount, brokerRunningCount, brokerCount, adminCount

    def hippoAdapter(self, action, configVersionPath=None, workDir=None, binaryPath=None):
        versionPath = ""
        if configVersionPath is not None:
            versionPath = configVersionPath
        else:
            versionPath = self.getLatestConfig(self.configPath)
        if versionPath == "":
            return False
        cmd = self.toolsConfig.getHippoAdapter() + ' %s %s' % (versionPath, action)
        if workDir is not None:
            adminStartDir = os.path.join(workDir, 'admin_starter_dir')
            if not os.path.exists(adminStartDir):
                os.makedirs(adminStartDir)
            cmd = "cd " + adminStartDir + "; " + cmd + " " + workDir
        if binaryPath is not None:
            cmd += " " + binaryPath
        cmd += " 2>>stderr.out"
        process = Process()
        out, err, code = process.run(cmd)
        if code != 0:
            errMsg = '%s admin failed: data[%s] error[%s] code[%d]' % (action, out, err, code)
            return False, errMsg
        return True, ''


class SwiftStartCmd(AppBaseCmd):
    '''
    swift {st|start} -z zfs://zfsaddr

options:
      -z zookeeper_address, --zookeeper=zookeeper_address   : required, zookeeper root address
      -w work_dir, --workdir=work_dir   : optional, local mode need
      -l log_conf, --logconf=log_conf   : optional, local mode need

examples:
    swift start -z zfs://zfsaddr
    '''

    def __init__(self, fromApi=False):
        super(SwiftStartCmd, self).__init__(fromApi)

    def addOptions(self):
        super(SwiftStartCmd, self).addOptions()
        self.parser.add_option('-v', '--configVersionPath', action='store', dest='configVersionPath')
        self.parser.add_option('-w', '--workDir', action='store', dest='workDir')
        self.parser.add_option('-b', '--binaryPath', action='store', dest='binaryPath')

    def initMember(self, options):
        super(SwiftStartCmd, self).initMember(options)
        self.configVersionPath = options.configVersionPath
        self.workDir = options.workDir
        self.binaryPath = options.binaryPath

    def run(self):
        print self.configVersionPath
        response = swift_proto_errcode.CommonResponse()
        response.errorInfo.errCode = swift_proto_errcode.ERROR_NONE
        ret, errMsg = self.hippoAdapter('start', self.configVersionPath,
                                        self.workDir, self.binaryPath)
        if not ret:
            response.errorInfo.errCode = swift_proto_errcode.ERROR_WARN_START
            response.errorInfo.errMsg = errMsg.replace('"', '\\"').replace('\n', ' |||| ')
            return False, response, errMsg
        return True, response, 0


class SwiftStopCmd(AppBaseCmd):
    '''
    swift {sp|stop} -z zfs://zfsaddr

options:
      -z zookeeper_address, --zookeeper=zookeeper_address   : required, zookeeper root address
      -w work_dir, --workdir=work_dir   : optional, local mode need

examples:
    swift stop -z zfs://10.250.12.23:1234/root
    '''

    def __init__(self, fromApi=False):
        super(SwiftStopCmd, self).__init__(fromApi)

    def addOptions(self):
        super(SwiftStopCmd, self).addOptions()
        self.parser.add_option('-w', '--workDir', action='store', dest='workDir')

    def initMember(self, options):
        super(SwiftStopCmd, self).initMember(options)
        self.workDir = options.workDir

    def run(self):
        response = swift_proto_errcode.CommonResponse()
        response.errorInfo.errCode = swift_proto_errcode.ERROR_NONE
        ret, errMsg = self.hippoAdapter('stop', workDir=self.workDir)
        if not ret:
            response.errorInfo.errCode = swift_proto_errcode.ERROR_UNKNOWN
            response.errorInfo.errMsg = errMsg.replace('"', '\\"').replace('\n', ' |||| ')
            return False, response, errMsg
        return True, response, 0


class SwiftUpdateAdminConfCmd(AppBaseCmd):
    '''
    swift {uac|updateAdminConf} -z zfs://zfsaddr

options:
      -z zookeeper_address, --zookeeper=zookeeper_address   : required, zookeeper root address
      -v config_version_path, --configVersionPath=config_version_path : optional, config version path

examples:
    swift uac -z zfs://zfsaddr
    '''

    def __init__(self, fromApi=False):
        super(SwiftUpdateAdminConfCmd, self).__init__(fromApi)

    def addOptions(self):
        super(SwiftUpdateAdminConfCmd, self).addOptions()
        self.parser.add_option('-v', '--configVersionPath', action='store', dest='configVersionPath')
        self.parser.add_option('-w', '--workDir', action='store', dest='workDir')
        self.parser.add_option('-b', '--binaryPath', action='store', dest='binaryPath')

    def initMember(self, options):
        super(SwiftUpdateAdminConfCmd, self).initMember(options)
        self.configVersionPath = options.configVersionPath
        self.workDir = options.workDir
        self.binaryPath = options.binaryPath

    def run(self):
        response = swift_proto_errcode.CommonResponse()
        response.errorInfo.errCode = swift_proto_errcode.ERROR_NONE
        ret, errMsg = self.hippoAdapter('updateAdminConf', self.configVersionPath,
                                        self.workDir, self.binaryPath)
        if not ret:
            response.errorInfo.errCode = swift_proto_errcode.ERROR_UNKNOWN
            response.errorInfo.errMsg = errMsg.replace('"', '\\"').replace('\n', ' |||| ')
            return False, response, errMsg
        return True, response, 0


class SwiftAppStatusCmd(AppBaseCmd):
    '''
    swift {gas|getappstatus}
        {-z zookeeper_address  | --zookeeper=zookeeper_address}
        {-i app_info          | --info=app_info}

    options:
       -z zookeeper_address, --zookeeper=zookeeper_address   : required, zookeeper root address
       -i app_info,    --info=app_info      : option[summary, all], print app info, default summary

examples:
    swift gas -z zfs://10.250.12.23:1234/root
    swift gas -z zfs://10.250.12.23:1234/root -i all
    '''

    def __init__(self):
        super(SwiftAppStatusCmd, self).__init__()

    def addOptions(self):
        super(SwiftAppStatusCmd, self).addOptions()
        self.parser.add_option('-i', '--info', action='store', dest='infoLevel')

    def checkOptionsValidity(self, options):
        if not super(SwiftAppStatusCmd, self).checkOptionsValidity(options):
            return False
        if options.infoLevel and options.infoLevel not in ['summary', 'all']:
            print "info value must in ['summary', 'all']"
            return False
        return True

    def initMember(self, options):
        super(SwiftAppStatusCmd, self).initMember(options)
        self.infoLevel = 'summary'
        if options.infoLevel:
            self.infoLevel = options.infoLevel

    def run(self):
        latestConfig = self.getLatestConfig(self.configPath)
        if latestConfig == "":
            return '', '', 1
        if not self.parseConfigInfo(latestConfig):
            return '', '', 1
        response = self.getAppInfo()
        if self.infoLevel == 'all':
            print response
            return response, '', 0
        if response.errorInfo.errorCode != 0:
            print "error code [%s], error msg [%s]" % (response.errorInfo.errorCode, response.errorInfo.errorMsg)
            return response.errorInfo.errorMsg, response.errorInfo.errorCode, 0

        (adminRunningCount, brokerRunningCount, brokerCount, adminCount) = self.parseWorkerCount(response)
        infoTemp = "admin [%d/%d/%s], broker [%d/%d/%s]"
        infoStr = infoTemp % (adminRunningCount, adminCount, self.adminCount,
                              brokerRunningCount, brokerCount, self.brokerCount)
        print "[RUNNING/ASSIGNED/REQUIRED]"
        print infoStr
        return infoStr, "", 0


class SwiftDeleteCmd(AppBaseCmd):
    '''
    swift {del|delete}
       {-z zookeeper_address  | --zookeeper=zookeeper_address}

    options:
       -z zookeeper_address, --zookeeper=zookeeper_address   : required, zookeeper root address

Example:
    swift del -z zfs://10.250.12.23:1234/root
    '''

    def __init__(self):
        super(SwiftDeleteCmd, self).__init__()

    def run(self):
        latestConfig = self.getLatestConfig(self.configPath)
        if latestConfig == "":
            return '', '', 1
        if not self.parseConfigInfo(latestConfig):
            return '', '', 1
        response = self.getAppInfo()
        (adminRunningCount, brokerRunningCount, brokerCount, adminCount) = self.parseWorkerCount(response)
        if adminCount > 0 or brokerCount > 0:
            print "swift is still running, zkPath:[%s], service name: [%s]."\
                % (self.zkRoot, self.appName)
            return "", "", 1

        print 'delete zkRoot:', self.zkRoot
        ret = self.deleteSwiftDirs(self.zkRoot)
        if ret:
            print "Delete zookeeper success!"
            return "", "", 0
        else:
            print "Delete zookeeper failed!"
            return "", "", 1

    def deleteSwiftDirs(self, zkRoot):
        try:
            pathList = self.fileUtil.listDir(zkRoot)
            if pathList is None:
                return True
            elif len(pathList) == 0:
                return True

            ret = True
            for path in pathList:
                fullPath = os.path.join(zkRoot, path)
                if not self.fileUtil.remove(fullPath):
                    print "ERROR: delete zookeeper address [%s] failed!" \
                        % fullPath
                    ret = False
            return ret
        except Exception as e:
            print str(e)
            return False


class SwiftKillBrokerCmd(AppBaseCmd):
    '''
    swift {kb|killbroker} -z zfs://zfsaddr -b default##broker_0_1

options:
      -z zookeeper_address, --zookeeper=zookeeper_address   : required, zookeeper root address
      -b broker_name, --broker=broker_name   : required, kill broker name
      -s signal, --signal=signal   : optional, default is 9
examples:
    swift start -z zfs://zfsaddr -b default##broker_0_1,default##broker_0_2
    swift start -z zfs://zfsaddr -b all
    swift start -z zfs://zfsaddr -f brokerNameFile.txt
    '''

    def __init__(self):
        super(SwiftKillBrokerCmd, self).__init__()

    def addOptions(self):
        super(SwiftKillBrokerCmd, self).addOptions()
        self.parser.add_option('-v', '--configVersionPath', action='store', dest='configVersionPath')
        self.parser.add_option('-b', '--brokerName', action='store', dest='brokerName')
        self.parser.add_option('-f', '--brokerFile', action='store', dest='brokerFile')
        self.parser.add_option('-s', '--signal', action='store', dest='signal')

    def checkOptionsValidity(self, options):
        if not super(SwiftKillBrokerCmd, self).checkOptionsValidity(options):
            return False
        if options.brokerName is None and options.brokerFile is None:
            print "ERROR: brokerName or brokerFile must specify one!"
            return False
        return True

    def initMember(self, options):
        super(SwiftKillBrokerCmd, self).initMember(options)
        self.configVersionPath = options.configVersionPath
        self.brokerName = options.brokerName
        if options.signal is not None:
            self.signal = int(options.signal)
        else:
            self.signal = 9
        if options.brokerFile is not None:
            if self.brokerName is None:
                self.brokerName = ''
            ff = open(options.brokerFile, 'r')
            for line in ff.readlines():
                self.brokerName += ',' + line.strip('\n')
            ff.close()

    def parseAppInfoToBrokerMap(self, appInfo):
        from hippo.proto.Common_pb2 import ProcessStatus
        brokerMap = {}
        for res in appInfo.lastAllocateResponse:
            tagName = res.resourceTag
            if tagName == '__internal_appmaster_resource_tag__':
                continue
            slot = res.assignedSlots[0]
            ip = slot.id.slaveAddress.split(':')[0]
            pid = -1
            if len(slot.processStatus) > 0 \
               and slot.processStatus[0].status == ProcessStatus.PS_RUNNING:
                pid = slot.processStatus[0].pid
            brokerMap[tagName] = (ip, pid)
        return brokerMap

    def killByPid(self, ip, pid, signal):
        cmd = 'ssh %s kill -%d %d' % (ip, signal, pid)
        print '  execute: ', cmd
        os.system(cmd)

    def run(self):
        # 1. get app all slot name and ip
        latestConfig = self.getLatestConfig(self.configPath)
        if latestConfig == "":
            return '', '', 1
        if not self.parseConfigInfo(latestConfig):
            return '', '', 1
        appInfo = self.getAppInfo()
        brokerMap = self.parseAppInfoToBrokerMap(appInfo)
        # 2. match ip and kill -9 pid
        if self.brokerName == 'all':
            for name in brokerMap.keys():
                ip = brokerMap[name][0]
                pid = brokerMap[name][1]
                print 'killing: %s [ip:%s, pid:%d]' % (name, ip, pid)
                if pid != -1:
                    self.killByPid(ip, pid, self.signal)
        else:
            for name in self.brokerName.split(','):
                name = name.strip()
                if len(name) == 0:
                    continue
                if brokerMap.has_key(name):
                    ip = brokerMap[name][0]
                    pid = brokerMap[name][1]
                    print 'killing: %s [ip:%s, pid:%d]' % (name, ip, pid)
                    if pid != -1:
                        self.killByPid(ip, pid, self.signal)
                else:
                    print name, 'not found'
        return '', '', 0


class GetBrokerStatusCmd(AppBaseCmd):
    '''
    swift {gbs|getbrokerstatus}
    options:
       -z zookeeper_address,     --zookeeper=zookeeper_address   : required, zookeeper root address
       -b broker,                --broker_role_name              : optional, role name

Example:
    swift gbs -z zfs://10.250.12.23:1234/root -b default#broker_1_2
    '''

    def addOptions(self):
        super(GetBrokerStatusCmd, self).addOptions()
        self.parser.add_option('-b', '--broker', action='store', dest='broker')

    def checkOptionsValidity(self, options):
        ret, errMsg = super(GetBrokerStatusCmd, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg
        return True, ''

    def initMember(self, options):
        super(GetBrokerStatusCmd, self).initMember(options)
        self.broker = options.broker

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)
        return True

    def run(self):
        ret, response, errorMsg = self.adminDelegate.getBrokerStatus(self.broker)
        if self.fromApi:
            return ret, response, errorMsg
        if not ret:
            print errorMsg
            return "", errorMsg, 1
        print response
        return response, "", 0


class UpdateWriterVersionCmd(AppBaseCmd):
    '''
    swift {uwv|updatewriterversion}
    options:
       -z zookeeper_address,     --zookeeper=zookeeper_address   : required, zookeeper root address
       -t topic_name,            --topic_name                    : required, topic name
       -r writer_name,           --writer_name                   : required, writer name
       -m major_version,         --major_version                 : required, [major_version]
       -n minor_version,         --minor_version                 : required, [minor_version]


Example:
    swift gbs -z zfs://10.250.12.23:1234/root -t test_topic -r role_1 -m 2 -n 2
    '''

    def addOptions(self):
        super(UpdateWriterVersionCmd, self).addOptions()
        self.parser.add_option('-t', '--topic_name', action='store', dest='topic_name')
        self.parser.add_option('-r', '--writer_name', action='store', dest='writer_name')
        self.parser.add_option('-m', '--major_version', action='store', dest='major_version')
        self.parser.add_option('-n', '--minor_version', action='store', dest='minor_version')

    def checkOptionsValidity(self, options):
        ret, errMsg = super(UpdateWriterVersionCmd, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg
        if not options.topic_name or not options.writer_name or not options.major_version or not options.minor_version:
            print "param invalid"
            return False
        return True, ''

    def initMember(self, options):
        super(UpdateWriterVersionCmd, self).initMember(options)
        self.topic_name = options.topic_name
        self.writer_name = options.writer_name
        self.major_version = options.major_version
        self.minor_version = options.minor_version

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)
        return True

    def run(self):
        ret, response, errorMsg = self.adminDelegate.updateWriterVersion(self.topic_name, self.writer_name,
                                                                         self.major_version, self.minor_version)
        if self.fromApi:
            return ret, response, errorMsg
        if not ret:
            print errorMsg
            return "", errorMsg, 1
        print response
        return response, "", 0


class TurnToMasterCmd(AppBaseCmd):
    '''
    swift {ttm|turntomaster}
    options:
       -z zookeeper_address,     --zookeeper=zookeeper_address   : required, zookeeper root address
       -v version,               --version                       : required, [targetVersion]

Example:
    swift ttm -z zfs://10.250.12.23:1234/root -v 123
    '''

    def addOptions(self):
        super(TurnToMasterCmd, self).addOptions()
        self.parser.add_option('-v', '--version', action='store', dest='version')

    def checkOptionsValidity(self, options):
        ret, errMsg = super(TurnToMasterCmd, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg
        if not options.version:
            print "param invalid"
            return False
        return True, ''

    def initMember(self, options):
        super(TurnToMasterCmd, self).initMember(options)
        self.version = options.version

    def buildDelegate(self):
        self.adminDelegate = swift_admin_delegate.SwiftAdminDelegate(
            self.fileUtil, self.zfsAddress, self.adminLeader, self.options.username, self.options.passwd)
        return True

    def run(self):
        ret, response, errorMsg = self.adminDelegate.turnToMaster(self.version)
        if self.fromApi:
            return ret, response, errorMsg
        if not ret:
            print errorMsg
            return "", errorMsg, 1
        print response
        return response, "", 0
