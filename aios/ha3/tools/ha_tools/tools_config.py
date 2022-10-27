#!/bin/env python
import sys
import os, shutil
import os.path
import my_config_parser
import tools_conf
from common_define import *
from process import *

class ToolsConfig(object):
    def __init__(self):
        self.fsUtilExe = tools_conf.install_prefix + "/bin/fs_util"
        if hasattr(tools_conf, "fs_util_exe"):
            self.fsUtilExe = tools_conf.fs_util_exe
        if not os.path.exists(self.fsUtilExe):
            print "ERROR: can't find fs_util_exe."
            sys.exit(-1)

        self.configUtilExe = tools_conf.install_prefix + "/bin/config_util"
        if hasattr(tools_conf, "config_util_exe"):
            self.configUtilExe = tools_conf.config_util_exe
        if not os.path.exists(self.configUtilExe):
            print "ERROR, can't find config_util"
            sys.exit(-1)

        self.configUtilLogFile = tools_conf.install_prefix + "/etc/ha3/config_util_alog.conf"

        self.localAgentClientExe = tools_conf.install_prefix + "/bin/local_agent_client"
        if hasattr(tools_conf, "local_agent_client_exe"):
            self.localAgentClientExe = tools_conf.local_agent_client_exe
        if not os.path.exists(self.localAgentClientExe):
            print "ERROR: can't find local_agent_client_exe."
            sys.exit(-1)

        self.indexDiffExe = tools_conf.install_prefix + "/bin/index_diff"
        if hasattr(tools_conf, "index_diff_exe"):
            self.indexDiffExe = tools_conf.index_diff_exe
        if not os.path.exists(self.indexDiffExe):
            print "ERROR: can't find index_diff_exe."
            sys.exit(-1)

        self.indexDiffLogFile = tools_conf.install_prefix + "/etc/ha_tools/index_diff_alog.conf"

        if hasattr(tools_conf, "lib_path"):
            self.libPath = tools_conf.lib_path
        else:
            print "ERROR: can't find lib_path in tools_conf.py"
            sys.exit(-1)

        if hasattr(tools_conf, "default_conf_path"):
            self.defaultConfPath = tools_conf.default_conf_path
        else:
            self.defaultConfPath = tools_conf.install_prefix + "/var/ha_tools/"

        if hasattr(tools_conf, "local_package_path"):
            self.localPackagePath = tools_conf.local_package_path
        else:
            self.localPackagePath = ""

        self.hadoopUser = tools_conf.hadoop_user
        self.hadoopGroup = tools_conf.hadoop_groups
        self.hadoopReplica = tools_conf.hadoop_replica
        if hasattr(tools_conf, HADOOP_JOB_BINARY_REPLICA_CONF_KEY):
            self.hadoopJobBinaryReplica = tools_conf.hadoop_job_binary_replica
        else:
            self.hadoopJobBinaryReplica = self.hadoopReplica

        if hasattr(tools_conf, "hadoop_home"):
            self.hadoopHome = tools_conf.hadoop_home
            if self.hadoopHome and not self.hadoopHome.endswith('/'):
                self.hadoopHome += '/'
        else:
            self.hadoopHome = ""

        if hasattr(tools_conf, "java_home"):
            self.javaHome = tools_conf.java_home
            if self.javaHome and not self.javaHome.endswith('/'):
                self.javaHome += '/'
        else:
            self.javaHome = ""

        self.retryDuration = 0
        if hasattr(tools_conf, "retry_duration"):
            self.retryDuration = tools_conf.retry_duration

        self.retryInterval = 0
        if hasattr(tools_conf, "retry_interval"):
            self.retryInterval = tools_conf.retry_interval

        self.retryTimes = 1
        if hasattr(tools_conf, "retry_times"):
            self.retryTimes = tools_conf.retry_times

    def __del__(self):
        pass

    def getHadoopHome(self):
        return self.hadoopHome

    def getJavaHome(self):
        return self.javaHome

    def getHadoopUserName(self):
        return self.hadoopUser

    def getHadoopGroupName(self):
        return self.hadoopGroup

    def getHadoopReplica(self):
        return self.hadoopReplica

    def getHadoopJobBinaryReplica(self):
        return self.hadoopJobBinaryReplica

    def getInstallPrefix(self):
        if not tools_conf.install_prefix.endswith('/'):
            return tools_conf.install_prefix + '/'
        return tools_conf.install_prefix

    def getHadoopBuilderExe(self):
        return self.hadoopBuilderExe

    def getHadoopBuilderLogConfFile(self):
        return self.hadoopBuilderLogConfFile

    def getAdminClientExe(self):
        return self.adminClientExe

    def getLocalAgentClientExe(self):
        return self.localAgentClientExe

    def getIndexDiffExe(self):
        return self.indexDiffExe

    def getIndexDiffLogFile(self):
        return self.indexDiffLogFile

    def getConfigUtilExe(self):
        return self.configUtilExe

    def getConfigUtilLogFile(self):
        return self.configUtilLogFile

    def getFsUtilExe(self):
        return self.fsUtilExe

    def getLocalBuilderExe(self):
        return self.localBuilderExe

    def getLocalMapperExe(self):
        return self.localMapperExe

    def getLibPath(self):
        return self.libPath

    def getDefaultConfPath(self):
        return self.defaultConfPath

    def getLocalPackagePath(self):
        return self.localPackagePath

    def getCmdEnv(self):
        cmdEnv = '/bin/env LD_LIBRARY_PATH=%(libPath)s ' % {\
            'libPath' : self.libPath}
        return cmdEnv

    def parse(self):
        return True

    def getRetryDuration(self):
        return self.retryDuration

    def getRetryInterval(self):
        return self.retryInterval

    def getRetryTimes(self):
        return self.retryTimes
