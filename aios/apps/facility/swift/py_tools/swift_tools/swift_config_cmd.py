#!/bin/env python

import os
import time
import random
import local_file_util

from process import Process
import base_cmd


class SwiftDeployCmd(base_cmd.BaseCmd):
    '''
    swift {deploy|dp}
       {-z zookeeper_address  | --zookeeper=zookeeper_address}
       {-l local_path     | --localPath=local_path}

options:
       -z zookeeper_address, --zookeeper=zookeeper_address   : required, zookeeper root address
       -l local_path, --localPath=local_path      : required, configuration path to be deployed

examples:
    swift dp -z zfs://10.250.12.23:1234/root -l /path/to/swift_conf
    '''

    def addOptions(self):
        super(SwiftDeployCmd, self).addOptions()
        self.parser.add_option('-l', '--localPath', action='store', dest='localPath')

    def checkOptionsValidity(self, options):
        ret, errMsg = super(SwiftDeployCmd, self).checkOptionsValidity(options)
        if not ret:
            return False, errMsg
        if not options.localPath:
            errMsg = 'Error: localPath must be specified'
            return False, errMsg
        return True, ''

    def initMember(self, options):
        super(SwiftDeployCmd, self).initMember(options)
        self.localPath = options.localPath

    def checkConfig(self, path):
        process = Process()
        cmd = self.toolsConfig.getConfigValidator() + ' %s' % path
        out, err, code = process.run(cmd)
        if code != 0:
            errMsg = '%s validate config failed: data[%s] error[%s] code[%d]'\
                     % (cmd, out, err, code)
            return False, errMsg
        return True, ''

    def run(self):
        response = {"errorInfo": {"errCode": "ERROR_DEPLOY", "errMsg": ""},
                    "version": 0}
        ret, errMsg = self.checkConfig(self.localPath)
        if not ret:
            response['errorInfo']['errMsg'] = errMsg.replace('"', '\\"').replace('\n', ' |||| ')
            return False, response, errMsg
        tempDirPath = os.path.join(self.configPath, 'deploy_temp_dir',
                                   '%.6f_%d' % (time.time(), random.randint(0, 10000)))

        if self.fileUtil.exists(tempDirPath):
            print 'WARN: %s already exists, removing...' % tempDirPath
            self.fileUtil.remove(tempDirPath)

        if not self.fileUtil.copy(self.localPath, tempDirPath):
            errMsg = "copy config from [%s] to [%s] failed" % (self.localPath, tempDirPath)
            response['errorInfo']['errMsg'] = errMsg
            return False, response, errMsg
        maxVersion = self.fileUtil.getMaxConfigVersion(self.configPath)
        curVersion = str(int(time.time()))
        remotePathWithVersion = os.path.join(self.configPath, curVersion)
        if not self.fileUtil.rename(tempDirPath, remotePathWithVersion):
            errMsg = "rename config from [%s] to [%s] failed" \
                     % (tempDirPath, remotePathWithVersion)
            response['errorInfo']['errMsg'] = errMsg
            return False, response, errMsg
        response['errorInfo']['errCode'] = 'ERROR_NONE'
        response['version'] = int(curVersion)
        print "deploy config success, max version is " + curVersion
        return True, response, 0


class SwiftDownloadCmd(SwiftDeployCmd):
    '''
    swift {dl|download}
       {-z zookeeper_address  | --zookeeper=zookeeper_address}
       {-l conf_path          | --config=config_path}

    options:
       -z zookeeper_address, --zookeeper=zookeeper_address   : required, zookeeper root address
       -l conf_path,         --config=conf_path              : required, download config from zookeeper

Example:
    swift dl -z zfs://10.250.12.23:1234/root -l /path/to/config
    '''

    def __init__(self):
        super(SwiftDownloadCmd, self).__init__()

    def addOptions(self):
        super(SwiftDownloadCmd, self).addOptions()
        self.parser.add_option('-v', '--version', action='store', dest='confVersion')

    def initMember(self, options):
        super(SwiftDownloadCmd, self).initMember(options)
        self.confVersion = None
        if options.confVersion is not None:
            self.confVersion = options.confVersion

    def run(self):
        maxVersion = -1
        if self.confVersion is not None:
            maxVersion = self.confVersion
        else:
            maxVersion = self.fileUtil.getMaxConfigVersion(self.configPath)
        remoteConfigPath = os.path.join(self.configPath, str(maxVersion))
        expectedLocalConfigPath = os.path.join(self.localPath, str(maxVersion))

        if self.fileUtil.exists(expectedLocalConfigPath):
            print 'Error, local dir [%s] already exist' % expectedLocalConfigPath
            return "", "", 1
        if not self.fileUtil.exists(remoteConfigPath):
            print 'Error, remote config dir  [%s] not exist' % remoteConfigPath
            return "", "", 1
        if not self.fileUtil.copy(remoteConfigPath, self.localPath):
            print "Error, download swift config from remote failed"
            return "", "", 1
        print "download swift config success, download version is  " + str(maxVersion)
        return "", "", 0


class SwiftCopyCmd(base_cmd.BaseCmd):
    '''
    swift {cp|copy}
       {-z zookeeper_address  | --zookeeper=zookeeper_address}
       {-l local_path         | --localpath=local_path}
       {-r remote_path        | --remotepath=remote_path}

    options:
       -z zookeeper_address, --zookeeper=zookeeper_address   : required, zookeeper root address
       -l local_path,        --localpath=conf_path           : required, local path
       -r remote_path        --remotepath=remote_path        : required, remote path

Example:
    swift cp -z zfs://10.250.12.23:1234/root -l /path/to/config -r remote_path
    '''

    def __init__(self):
        super(SwiftCopyCmd, self).__init__()

    def addOptions(self):
        super(SwiftCopyCmd, self).addOptions()
        self.parser.add_option('-l', '--localpath', action='store', dest='localPath')
        self.parser.add_option('-r', '--remotepath', action='store', dest='remotePath')

    def checkOptionsValidity(self, options):
        if not super(SwiftCopyCmd, self).checkOptionsValidity(options):
            return False
        if not options.localPath:
            print 'Error: localPath must be specified'
            return False
        if not options.remotePath:
            print 'Error: localPath must be specified'
            return False
        return True

    def initMember(self, options):
        super(SwiftCopyCmd, self).initMember(options)
        self.localPath = os.path.join(self.zfsAddress, options.localPath)
        self.remotePath = options.remotePath

    def run(self):
        if not self.fileUtil.copy(self.localPath, self.remotePath, True):
            print "Error, copy file from %s to %s failed" % \
                (self.localPath, self.remotePath)
            return "", "", 1
        print "copy file from %s to %s success" % \
            (self.localPath, self.remotePath)
        return "", "", 0


class SwiftVersionCmd(base_cmd.BaseCmd):
    '''
    swift {cv|configversion}
       {-z zookeeper_address  | --zookeeper=zookeeper_address}

    options:
       -z zookeeper_address, --zookeeper=zookeeper_address   : required, zookeeper root address

Example:
    swift cv -z zfs://10.250.12.23:1234/root
    '''

    def addOptions(self):
        super(SwiftVersionCmd, self).addOptions()

    def initMember(self, options):
        super(SwiftVersionCmd, self).initMember(options)
        self.localFileUtil = local_file_util.LocalFileUtil()

    def run(self):
        versionFile = self.zfsAddress + "/config/version"
        localFile = "./.version"
        self.fileUtil.copy(versionFile, localFile, True)
        versionStr = self.localFileUtil.cat(localFile)
        if not self.fromApi:
            print versionStr
        return True, versionStr, 0


class GetConfigCmd(base_cmd.BaseCmd):
    '''
    swift {gc|getconfig}
       {-z zookeeper_address  | --zookeeper=zookeeper_address}

    options:
       -z zookeeper_address, --zookeeper=zookeeper_address   : required, zookeeper root address

    Example:
    swift gc -z zfs://10.250.12.23:1234/root
    '''

    def addOptions(self):
        super(GetConfigCmd, self).addOptions()

    def initMember(self, options):
        super(GetConfigCmd, self).initMember(options)
        self.localFileUtil = local_file_util.LocalFileUtil()

    def run(self):
        '''
        {
        "admin_current": "zfs://10.101.169.99:13191/swift/swift_api_test/config/1576486442",
        "broker_current": "zfs://10.101.169.99:13191/swift/swift_api_test/config/1576489804",
        "broker_current_role_version": "0",
        "broker_target": "zfs://10.101.169.99:13191/swift/swift_api_test/config/1576489804",
        "broker_target_role_version": "0",
        "rollback": false
        }
        '''
        import json
        versionFile = self.zfsAddress + "/config/version"
        localFile = "./.version"
        self.fileUtil.copy(versionFile, localFile, True)
        versionStr = self.localFileUtil.cat(localFile)
        version = json.loads(versionStr)
        adminCurVer = version['admin_current'].split('/')[-1]
        brokerCurVer = version['broker_current'].split('/')[-1]
        errMsg = ''
        response = {"swiftConfig": "", "hippoConfig": {},
                    "errorInfo": {"errCode": "ERROR_NONE", "errMsg": "ERROR_NONE"}}
        if adminCurVer != brokerCurVer:
            errMsg = 'admin version[%s] not equal to broker version[%s]' \
                     % (adminCurVer, brokerCurVer)
            response['errorInfo']['errCode'] = 'ERROR_VERSION'
            response['errorInfo']['errMsg'] = errMsg
            return errMsg, response, 1
        swiftConf = version['broker_current'] + '/swift.conf'
        hippoConf = version['broker_current'] + '/swift_hippo.json'
        self.fileUtil.copy(swiftConf, './swiftconf', True)
        self.fileUtil.copy(hippoConf, './hippoconf', True)
        response['swiftConfig'] = self.localFileUtil.cat('./swiftconf')
        response['hippoConfig'] = self.localFileUtil.cat('./hippoconf')
        return True, response, 0
