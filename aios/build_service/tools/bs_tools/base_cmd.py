import sys
import os
import optparse

import fs_util_delegate

class BaseCmd(object):
    def __init__(self):
        self.parser = optparse.OptionParser(usage=self.__doc__)
        import tools_config
        self.toolsConf = tools_config.ToolsConfig()
        self.fsUtil = fs_util_delegate.FsUtilDelegate(self.toolsConf)

    def parseParams(self, optionList):
        self.addOptions()
        (options, args) = self.parser.parse_args(optionList)
        
        self.checkOptionsValidity(options)
        self.initMember(options)

    def addOptions(self):
        pass
    
    def checkOptionsValidity(self, options):
        pass

    def initMember(self, options):
        pass

    def usage(self):
        print self.__doc__ % {'prog' : sys.argv[0]}

    def run(self):
        return False

    def getLatestConfig(self, configPath):
        version = self.fsUtil.getMaxConfigVersion(configPath)
        if version == -1:
            raise Exception('Invalid config path[%s].' % (configPath))
        return os.path.join(configPath, str(version))
