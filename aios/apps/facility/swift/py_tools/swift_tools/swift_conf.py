#!/bin/env python
import string
import os
import sys
import tempfile
import traceback
import ConfigParser
import swift_common_define
import local_file_util
import swift.protocol.Common_pb2 as swift_proto_common

from swift_util import SwiftUtil


class SwiftConfig:
    def __init__(self, rootPath, fileUtil, confSubDir=True):
        self.rootPath = fileUtil.normalizeDir(rootPath)
        self.fileUtil = fileUtil
        if confSubDir:
            self.configRoot = self.rootPath + "config"
        else:
            self.configRoot = self.rootPath
        self.swiftCommonDict = {}
        self.swiftAdminDict = {}
        self.swiftBrokerDict = {}

    def parse(self):
        swiftConfig = self._getConfigFilePath(swift_common_define.SWIFT_CONF_FILE_NAME)
        if swiftConfig is None:
            return False

        try:
            if not self.fileUtil.exists(swiftConfig):
                print "ERROR: swift config file [%s] does not exist! please deploy config first." % swiftConfig
                return False
        except Exception as e:
            print str(e)
            return False

        content = self.fileUtil.cat(swiftConfig)
        if (content is None or content == ""):
            return False

        if not self._parseSwiftConfContent(content):
            return False

        return True

    def validateConf(self):
        if not self._validateCommonSection():
            return False
        if not self._validateAdminSection():
            return False
        if not self._validateBrokerSection():
            return False
        return True

    def _validateCommonSection(self):
        for field in swift_common_define.SWIFT_CONF_COMMON_SECTION_NECESSARY_FIELDS:
            if not self.swiftCommonDict.has_key(field):
                print "ERROR: could not find [%s] in common section of swift.conf" \
                    % field
                return False
        return True

    def _validateAdminSection(self):
        for field in swift_common_define.SWIFT_CONF_ADMIN_SECTION_NECESSARY_FIELDS:
            if not self.swiftAdminDict.has_key(field):
                print "ERROR: could not find [%s] in admin section of swift.conf"\
                    % field
                return False
        return True

    def _validateBrokerSection(self):
        for field in swift_common_define.SWIFT_CONF_BROKER_SECTION_NECESSARY_FIELDS:
            if not self.swiftBrokerDict.has_key(field):
                print "ERROR: could not find [%s] in broker section of swift.conf" \
                    % field
                return False
        return True

    def getSwiftBinPath(self):
        installPrefix = self._getInstallPrefix()
        if installPrefix is not None:
            return installPrefix + "bin/"
        return None

    def getSwiftAdminExePath(self):
        swiftBinPath = self.getSwiftBinPath()
        if swiftBinPath:
            return os.path.join(swiftBinPath,
                                swift_common_define.SWFIT_ADMIN_EXE)
        return None

    def getSwiftBrokerExePath(self):
        swiftBinPath = self.getSwiftBinPath()
        if swiftBinPath:
            return os.path.join(swiftBinPath,
                                swift_common_define.SWFIT_BROKER_EXE)
        return None

    def getLogConfFile(self):
        if self.swiftCommonDict.has_key(swift_common_define.SWIFT_CONF_LOG_CONF):
            return self.swiftCommonDict[swift_common_define.SWIFT_CONF_LOG_CONF]
        elif self.swiftCommonDict.has_key(swift_common_define.SWIFT_CONF_INSTALL_PREFIX):
            return self.fileUtil.normalizeDir(self.swiftCommonDict[swift_common_define.SWIFT_CONF_INSTALL_PREFIX]) \
                + "etc/swift/swift_alog.conf"
        else:
            return None

    def getHadoopHome(self):
        if self.swiftCommonDict.has_key(swift_common_define.SWIFT_CONF_HADOOP_HOME):
            return self.swiftCommonDict[swift_common_define.SWIFT_CONF_HADOOP_HOME]
        else:
            return None

    def _getAdminLeaderInfoFile(self):
        return self.fileUtil.normalizeDir(self.rootPath) \
            + "admin/leader_info"

    def _getAdminLeaderHisFile(self):
        return self.fileUtil.normalizeDir(self.rootPath) \
            + "admin/leader_history"

    def setAdminLeaderInfoStarted(self):
        adminLeaderFile = self._getAdminLeaderInfoFile()
        if not self.fileUtil.exists(adminLeaderFile):
            return True

        leaderInfo = self.getAdminLeaderInfo()
        if not leaderInfo:
            return True
        if not leaderInfo.sysStop:
            return True

        # make sysStop = False
        leaderInfo = swift_proto_common.LeaderInfo()
        leaderInfo.sysStop = False
        serialedStr = leaderInfo.SerializeToString()

        tmpFile = tempfile.NamedTemporaryFile()
        tmpFile.write(serialedStr)
        tmpFile.seek(0)
        if not self.fileUtil.copy(tmpFile.name, adminLeaderFile, True):
            tmpFile.close()
            print "ERROR: copy file from [%s] to [%s] failed!" \
                % (tmpFile.name, adminLeaderFile)
            return False

        tmpFile.close()
        return True

    def getAdminLeaderHistory(self):
        adminLeaderHisFile = self._getAdminLeaderHisFile()
        content = self._getZfsFileContent(adminLeaderHisFile)
        if not content:
            return None

        leaderHis = swift_proto_common.LeaderInfoHistory()

        try:
            leaderHis.ParseFromString(content)
        except Exception as e:
            print "ERROR: parse LeaderInfoHistory from string[%s] failed!" \
                % content
            return None

        return leaderHis

    def getAdminLeaderInfo(self):
        adminLeaderFile = self._getAdminLeaderInfoFile()
        content = self._getZfsFileContent(adminLeaderFile)
        if not content:
            return None

        leaderInfo = swift_proto_common.LeaderInfo()

        try:
            leaderInfo.ParseFromString(content)
        except Exception as e:
            print "ERROR: parse leaderInfo from string[%s] failed!" % content
            return None

        return leaderInfo

    def _getZfsFileContent(self, zfsPath):
        if not self.fileUtil.exists(zfsPath):
            return None

        tmpFile = tempfile.NamedTemporaryFile()
        if not self.fileUtil.copy(zfsPath, tmpFile.name, True):
            tmpFile.close()
            return None

        localFileUtil = local_file_util.LocalFileUtil()
        content = localFileUtil.read(tmpFile.name)
        tmpFile.close()
        return content

    def getSwiftLibPath(self):
        if self.swiftCommonDict.has_key(swift_common_define.SWIFT_CONF_LIB_PATH):
            return self.swiftCommonDict[swift_common_define.SWIFT_CONF_LIB_PATH]
        return self._getDefaultLibPath()

    def _getInstallPrefix(self):
        if self.swiftCommonDict.has_key(swift_common_define.SWIFT_CONF_INSTALL_PREFIX):
            return self.fileUtil.normalizeDir(self.swiftCommonDict[swift_common_define.SWIFT_CONF_INSTALL_PREFIX])
        else:
            return None

    def _getDefaultLibPath(self):
        installPrefix = self._getInstallPrefix()
        if installPrefix is not None:
            return installPrefix + "lib/:" + installPrefix + \
                "lib64/:/usr/local/lib/:/usr/local/lib64/:/usr/lib/" + \
                ":/usr/lib64/:/usr/ali/java/jre/lib/amd64/server/"
        return None

    def _getConfigFilePath(self, configFileName):
        if self.configRoot is None:
            return None
        return self.fileUtil.normalizeDir(self.configRoot) + configFileName

    def _parseSwiftConfContent(self, content):
        if content is None or content == "":
            return False
        configParser = ConfigParser.ConfigParser()
        tmpFile = tempfile.NamedTemporaryFile()
        tmpFile.write(content)
        tmpFile.seek(0)
        configParser.readfp(tmpFile)
        if not configParser.has_section(swift_common_define.SWIFT_CONF_COMMON_SECTION_NAME):
            print "ERROR: without section[common] in swift.conf"
            tmpFile.close()
            return False
        for (name, value) in configParser.items(swift_common_define.SWIFT_CONF_COMMON_SECTION_NAME):
            self.swiftCommonDict[name] = value

        if not configParser.has_section(swift_common_define.SWIFT_CONF_ADMIN_SECTION_NAME):
            print "ERROR: without section[%s] in swift.conf" \
                % swift_common_define.SWIFT_CONF_ADMIN_SECTION_NAME
            tmpFile.close()
            return False
        for name, value in configParser.items(swift_common_define.SWIFT_CONF_ADMIN_SECTION_NAME):
            self.swiftAdminDict[name] = value

        if not configParser.has_section(swift_common_define.SWIFT_CONF_BROKER_SECTION_NAME):
            print "ERROR: without section[%s] in swift.conf" \
                % swift_common_define.SWIFT_CONF_BROKER_SECTION_NAME
            tmpFile.close()
            return False
        for name, value in configParser.items(swift_common_define.SWIFT_CONF_BROKER_SECTION_NAME):
            self.swiftBrokerDict[name] = value

        tmpFile.close()

        return True

    def getAdminConf(self, key):
        if self.swiftAdminDict.has_key(key):
            return self.swiftAdminDict[key]
        return ""

    def getBrokerConf(self, key):
        if self.swiftBrokerDict.has_key(key):
            return self.swiftBrokerDict[key]
        return ""
