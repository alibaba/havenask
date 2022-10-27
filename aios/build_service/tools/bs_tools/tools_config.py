#!/bin/env python
import sys
import os, shutil
import os.path

import tools_conf
import process

class ToolsConfig(object):
    def __init__(self):
        if not hasattr(tools_conf, 'install_prefix'):
            raise Exception("ERROR: can't find install prefix.")
            sys.exit(-1)
        self.install_prefix = tools_conf.install_prefix
        if not os.path.exists(self.install_prefix):
            raise Exception("ERROR: can't find install prefix[%s]." % self.install_prefix)
            sys.exit(-1)

        if hasattr(tools_conf, 'lib_path'):
            self.lib_path = tools_conf.lib_path
        else:
            raise Exception("ERROR: can't find lib_path in tools_conf.py")

        self.hadoop_user = tools_conf.hadoop_user
        self.hadoop_group = tools_conf.hadoop_groups
        self.hadoop_replica = tools_conf.hadoop_replica
        if hasattr(tools_conf, 'hadoop_home'):
            self.hadoop_home = tools_conf.hadoop_home
            if self.hadoop_home and not self.hadoop_home.endswith('/'):
                self.hadoop_home += '/'
        else:
            self.hadoop_home = ''

        if hasattr(tools_conf, 'java_home'):
            self.java_home = tools_conf.java_home
            if self.java_home and not self.java_home.endswith('/'):
                self.java_home += '/'
        else:
            self.java_home = ''

        self.vipclient_set_failover_path = ''
        if hasattr(tools_conf, 'vipclient_set_failover_path'):
            self.vipclient_set_failover_path = tools_conf.vipclient_set_failover_path

        self.pm_run_exe = self.__getFromToolsConfWithDefault('pm_run', os.path.join(self.install_prefix, 'bin/apsara_tool/pm_run'))
        self.rpc_caller_exe = self.__getFromToolsConfWithDefault('rpc_caller', os.path.join(self.install_prefix, 'bin/apsara_tool/rpc_caller'))

        self.retry_duration = self.__getFromToolsConfWithDefault('retry_duration', 0)
        self.retry_interval = self.__getFromToolsConfWithDefault('retry_interval', 0)
        self.retry_times = self.__getFromToolsConfWithDefault('retry_times', 1)

    def __getFromToolsConfWithDefault(self, name, defaultValue = None):
        if hasattr(tools_conf, name):
            return getattr(tools_conf, name)
        return defaultValue

    def __getCmd(self, cmd):
        envCmd = ''
        if self.lib_path:
            lib_path = self.lib_path
            if self.java_home:
                lib_path += ':' + os.path.join(self.java_home,
                                               'jre/lib/amd64/server/')
            envCmd += ' LD_LIBRARY_PATH=%s ' % lib_path
        if self.hadoop_home:
            envCmd += ' HADOOP_HOME=%s ' % self.hadoop_home
        if self.java_home:
            envCmd += ' JAVA_HOME=%s ' % self.java_home
        if self.vipclient_set_failover_path:
            envCmd += ' VIPCLIENT_SET_FAILOVER_PATH=%s ' % self.vipclient_set_failover_path
        if envCmd:
            envCmd = '/bin/env' + envCmd
        return envCmd + cmd + ' '

    def getClassPath(self):
        if not self.hadoop_home:
            return ''

        if not os.path.exists(self.hadoop_home):
            print 'WARN: hadoopHome[%s] not exist' % self.hadoop_home
            return ''

        classPathUtil = self.__getFromToolsConfWithDefault('class_path_util', self.install_prefix + '/bin/class_path_util')
        if not os.path.exists(classPathUtil):
            raise Exception("ERROR: can't find class_path_util")

        classPathUtilCmd = self.__getCmd(classPathUtil)
        p = process.Process()
        out, err, code = p.run('%s %s' % (classPathUtilCmd, self.hadoop_home))
        if code != 0:
            raise Exception('ERROR: failed to get classpath, cmd[%s], error[%s]' % (classPathUtilCmd, err))
        return out.rstrip('\n')

    def getHippoAdapter(self):
        return None

    def getFsUtil(self):
        return self.__getCmd('/ha3_depends/usr/local/bin/fs_util')

    def getConfigValidator(self):
        return self.__getCmd(self.install_prefix + '/bin/bs_config_validator')

    def getGenerationScanner(self):
        return self.__getCmd(self.install_prefix + '/bin/bs_generation_scanner')

    def getPartitionSplitMerger(self):
        return self.__getCmd(self.install_prefix + '/bin/bs_partition_split_merger')

    def getLocalJob(self):
        return self.__getCmd(self.install_prefix + '/bin/bs_local_job')
