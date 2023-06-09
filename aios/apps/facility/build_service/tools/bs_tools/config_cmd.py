#!/bin/env python

import time
import random

from process import Process
import base_cmd

class DeployCmd(base_cmd.BaseCmd):
    '''
    bs {deploy|dp}
        {-l local_path     | --localPath=local_path}
        {-r remote_path    | ---remotePath=remote_path}

options:
    -l local_path, --localPath=local_path      : required, configuration path to be deployed
    -r remote_path, --remotePath=remote_path   : required, remote path for configuration

examples:
    bs dp -r pangu://10.125.224.28:10240/home/liukan -l /home/liukan/tools/local_config
    bs deploy --remotePath=pangu://10.125.224.28:10240/home/liukan/ --localPath=/home/liukan/tools/local_config
    '''

    def __init__(self):
        super(DeployCmd, self).__init__()

    def addOptions(self):
        self.parser.add_option('-l', '--localPath', action='store', dest='localPath')
        self.parser.add_option('-r', '--remotePath', action='store', dest='remotePath')

    def checkOptionsValidity(self, options):
        if not options.localPath or not options.remotePath:
            raise Exception('localPath and remotePath must be specified')

    def initMember(self, options):
        super(DeployCmd, self).initMember(options)
        self.localPath = options.localPath
        self.remotePath = options.remotePath

    def checkConfig(self):
        process = Process()
        cmd = self.toolsConf.getConfigValidator() + ' %s' % self.localPath
        out, err, code = process.run(cmd)
        if code != 0:
            raise Exception('%s validate config failed: data[%s] error[%s] code[%d]' %
                            (cmd, out, err, code))

    def run(self):
        self.checkConfig()

        tempDirPath = "%sdeploy_temp_dir/%.6f_%d" % \
            (self.fsUtil.normalizeDir(self.remotePath), time.time(), random.randint(0,10000))

        if self.fsUtil.exists(tempDirPath):
            print 'WARN: %s already exists, removing...' % tempDirPath
            self.fsUtil.remove(tempDirPath)

        self.fsUtil.copy(self.localPath, tempDirPath)
        curVersion = int(time.time())
        remotePathWithVersion = self.fsUtil.normalizeDir(self.remotePath) + str(curVersion)
        self.fsUtil.rename(tempDirPath, remotePathWithVersion)

class DownloadCmd(DeployCmd):
    '''
    bs {downloadconfig|dl}
        {-l local_path     | --localPath=local_path}
        {-r remote_path    | ---remotePath=remote_path}

options:
    -l local_path, --localPath=local_path      : required, configuration path to be deployed
    -r remote_path, --remotePath=remote_path   : required, remote path for configuration

examples:
    bs dl -r pangu://PANGGU/home/wanggf -l /home/wanggf/tools/local_config
    bs downloadconfig --remotePath=pangu://PANGU/home/wanggf/ --localPath=/home/wanggf/tools/local_config
    '''

    def __init__(self):
        super(DownloadCmd, self).__init__()

    def run(self):
        maxVersion = self.fsUtil.getMaxConfigVersion(self.remotePath)
        latestPath = self.fsUtil.normalizeDir(self.remotePath) + str(maxVersion)
        expectedLocalConfigPath = self.fsUtil.normalizeDir(self.localPath) \
            + str(maxVersion)

        if self.fsUtil.exists(expectedLocalConfigPath):
            raise Exception('local dir [%s] already exist' % expectedLocalConfigPath)

        self.fsUtil.copy(latestPath, self.localPath)

class ValidateConfigCmd(base_cmd.BaseCmd):
    '''
    bs {validateconfig|vc}
        {-l local_path     | --localPath=local_path}

options:
    -l local_path, --localPath=local_path      : required, configuration path to be validated

examples:
    bs vc -l pangu://10.125.224.28:10240/home/config/9
    bs vc -l ~/local_config
    '''
    def addOptions(self):
        self.parser.add_option('-l', '--localPath', action='store', dest='localPath')

    def checkOptionsValidity(self, options):
        if not options.localPath:
            raise Exception('localPath must be specified')

    def initMember(self, options):
        self.localPath = options.localPath

    def checkConfig(self):
        process = Process()
        cmd = self.toolsConf.getConfigValidator() + ' %s' % self.localPath
        out, err, code = process.run(cmd)
        if code != 0:
            raise Exception('%s validate config failed: data[%s] error[%s] code[%d]' %
                            (cmd, out, err, code))

    def run(self):
        self.checkConfig()
