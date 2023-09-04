import os
import sys
import optparse
import time
import re

import tools_config
import fs_util_delegate
import swift_util
import swift_common_define
import swift_authority_checker
import api_optparse


class BaseCmd(object):
    def __init__(self, fromApi=False):
        if fromApi:
            self.parser = api_optparse.ApiOptionParser(usage=self.__doc__)
        else:
            self.parser = optparse.OptionParser(usage=self.__doc__)
        self.options = None
        self.toolsConfig = None
        self.fromApi = fromApi

    def parseParams(self, optionList):
        self.optionList = optionList

        self.addOptions()
        try:
            (options, args) = self.parser.parse_args(optionList)
        except Exception as e:
            return False, str(e)
        self.options = options
        self.args = args

        ret, errMsg = self.checkOptionsValidity(options)
        if not ret:
            errMsg += " ERROR: checkOptionsValidity Failed!"
            return False, errMsg

        self.initMember(options)

        ret, errMsg = self.buildToolsConfig()
        if not ret:
            errMsg += "ERROR: buildToolsConfig Failed!"
            return False, errMsg

        ret = self.buildFileUtil()
        if not ret:
            errMsg = "ERROR: buildFileUtil Failed!"
            return False, errMsg

        ret = self.buildAuthorityCheckor()
        if not ret:
            errMsg = "ERROR: buildAuthorityChecker Failed!"
            return False, errMsg

        ret = self.individualLogic()
        if not ret:
            errMsg = "ERROR: individualLogic Failed!"
            return False, errMsg

        ret = self.buildDelegate()
        if not ret:
            errMsg = "ERROR: buildDelegate Failed!"
            return False, errMsg

        return True, errMsg

    def checkAuthority(self):
        if self.options.closeAuthority is None:
            return self.authorityChecker.validate()
        return True

    def run(self):
        return ("", "", 0)

    def addOptions(self):
        self.parser.add_option('-z', '--zookeeper',
                               action='store', dest='zfsAddress')

        # used by function test case
        self.parser.add_option('', '--adminleader',
                               action='store', dest='adminLeader')
        # close authority check
        self.parser.add_option('', '--closeAuthority',
                               action='store', dest='closeAuthority')
        self.parser.add_option('-U', '--username', action='store', dest='username', default="")
        self.parser.add_option('-A', '--passwd', action='store', dest='passwd', default="")
        self.parser.add_option('-D', '--accessId', action='store', dest='accessId', default="")
        self.parser.add_option('-K', '--accessKey', action='store', dest='accessKey', default="")

    def checkOptionsValidity(self, options, needZk=True):
        if not options.zfsAddress and needZk:
            errMsg = "ERROR: zookeeper address must be specified!"
            return False, errMsg

        if options.adminLeader:
            tmpStr = options.adminLeader.strip()
            m = re.match("(.*):[0-9]+", tmpStr)
            if m is None:
                errMsg = "ERROR: admin leader address is invalid! Its format should be ip:port"
                return False, errMsg
            ipStr = m.group(1)
            if not swift_util.SwiftUtil.checkIpAddress(ipStr):
                errMsg = "ERROR: admin leader ip addressp[%s] is invalid! " % ipStr
                return False, errMsg

        return True, ''

    def initMember(self, options):
        self.zfsAddress = options.zfsAddress
        self.configPath = os.path.join(self.zfsAddress, 'config')
        self.adminLeader = None
        if options.adminLeader:
            self.adminLeader = options.adminLeader.strip()

    def buildToolsConfig(self):
        self.toolsConfig = tools_config.ToolsConfig()
        if not self.toolsConfig.parse():
            errMsg = "ERROR: parse tools config failed!"
            return False, errMsg
        return True, ''

    def buildFileUtil(self):
        self.fileUtil = fs_util_delegate.FsUtilDelegate(
            self.toolsConfig.getFsUtilExe(),
            self.toolsConfig.getLibPath())
        if not self.fileUtil:
            return False
        return True

    def buildAuthorityCheckor(self):
        if self.zfsAddress is None:
            return True
        self.authorityChecker = swift_authority_checker.SwiftAuthorityCheckor(
            self.zfsAddress, self.fileUtil)
        if not self.authorityChecker:
            return False
        return True

    def individualLogic(self):
        return True

    def buildDelegate(self):
        return True

    def getAdminLeaderAddr(self):
        if self.adminLeader:
            # just for function test case
            print "WARN: using user configured admin leader[%s], "\
                "just for function test case." % self.adminLeader
            return True, "tcp:" + self.adminLeader

        adminSpec = None
        adminStopped = True

        i = 0
        sleepMs = 0.01
        retryTime = swift_common_define.GET_ADMIN_LEADER_TRY_TIMES * 100
        while (i < retryTime):
            try:
                adminLeaderInfo = self.swiftConf.getAdminLeaderInfo()
            except BaseException:
                print "get admin leader info failed for[%d] times" % (i)
                i = i + 1
                time.sleep(sleepMs)
                continue

            if adminLeaderInfo:
                if adminLeaderInfo.HasField(swift_common_define.PROTO_ADDRESS):
                    adminSpec = adminLeaderInfo.address
                if adminLeaderInfo.HasField(swift_common_define.PROTO_SYS_STOP):
                    adminStopped = adminLeaderInfo.sysStop
                if (not adminStopped) and adminSpec:
                    return True, "tcp:" + adminSpec
            i = i + 1
            time.sleep(sleepMs)

        print "ERROR: could not get admin leader address! You should start swift system first!"
        return False, None

    def usage(self):
        print self.__doc__ % {'prog': sys.argv[0]}

    def getLatestConfig(self, configPath):
        version = self.fileUtil.getMaxConfigVersion(configPath)
        if version == -1:
            raise Exception('Error: Invalid config path[%s].' % (configPath))
        return os.path.join(configPath, str(version))
