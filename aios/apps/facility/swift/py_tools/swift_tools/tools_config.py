import os
import ConfigParser
import tools_conf
from swift_common_define import *
from swift_util import SwiftUtil

class ToolsConfig:
    def parse(self):
        if hasattr(tools_conf, SWIFT_TOOLS_CONFIG_INSTALL_PERFIX):
            self.installPrefix = tools_conf.install_prefix
        else:
            print "ERROR: could not find install_prefix in tools_conf.py!"
            return False
        if not os.path.exists(self.installPrefix):
            print "ERROR: install_prefix [%s] does not exist!" % self.installPrefix
            return False

        self.fsUtilExe = os.path.join(tools_conf.install_prefix, "bin/fs_util")
        if hasattr(tools_conf, SWIFT_TOOLS_CONFIG_FS_UTIL_EXE):
            self.fsUtilExe = tools_conf.fs_util_exe
        if not os.path.exists(self.fsUtilExe):
            print "ERROR: fs_util [%s] does not exist!" % self.fsUtilExe
            return False

        if hasattr(tools_conf, SWIFT_TOOLS_CONFIG_LIB_PATH):
            self.libPath = tools_conf.lib_path
        else:
            print "ERROR: could not find lib_path in tools_conf.py"
            return False

        if hasattr(tools_conf, SWIFT_TOOLS_CONFIG_HADOOP_HOME):
            self.hadoopHome = tools_conf.hadoop_home
        else:
            self.hadoopHome = None

        if hasattr(tools_conf, SWIFT_TOOLS_CONFIG_CLASS_PATH_UTIL):
            self.classPathUtil = tools_conf.class_path_util
        else:
            self.classPathUtil = self.installPrefix + '/bin/class_path_util'
        if not os.path.exists(self.classPathUtil):
            print "ERROR: class_path_util [%s] does not exist!" % self.classPathUtil
            return False
        self.useAsan = True
        return True

    def getHippoAdapter(self):
        return self.__getCmd(self.libPath, self.hadoopHome,
                             self.installPrefix + '/bin/swift_admin_starter')

    def getConfigValidator(self):
        return self.__getCmd(self.libPath, self.hadoopHome,
                             self.installPrefix + '/bin/swift_config_validator')

    def getInstallPrefix(self):
        return self.installPrefix

    def getFsUtilExe(self):
        return self.fsUtilExe

    def getLibPath(self):
        return self.libPath

    def getCmdEnv(self):
        return "/bin/env LD_LIBRARY_PATH=%s" % self.libPath

    def getHadoopHome(self):
        return self.hadoopHome

    def getClassPathUtil(self):
        return self.classPathUtil

    def __getCmd(self, libPath, hadoopHome, cmd):
        envCmd = ''
        if self.useAsan:
            envCmd += ' LSAN_OPTIONS=suppressions=%s/etc/swift/leak_suppression ASAN_OPTIONS=\"handle_segv=0 disable_coredump=0\"' % self.installPrefix 
        if libPath:
            envCmd += ' LD_LIBRARY_PATH=%s ' % libPath
        if hadoopHome:
            envCmd += ' HADOOP_HOME=%s ' % hadoopHome
        if envCmd:
            envCmd = '/bin/env' + envCmd
        return envCmd + cmd + ' '
