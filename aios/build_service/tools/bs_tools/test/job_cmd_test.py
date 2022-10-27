import unittest
from tools_test_common import *

import sys
import os
curPath = os.path.split(os.path.realpath(__file__))[0]
parentPath = curPath + "/../"
sys.path.append(parentPath)

from fs_util_delegate import *
from tools_config import *
from job_cmd import *

class StartJobCmdTest(unittest.TestCase):
    def setUp(self):
        self.workDir = TEST_DATA_TEMP_PATH + "startjob_test/"
        self.configPath = TEST_DATA_PATH + "job_delegate_test/config/"
        self.toolsConfig = ToolsConfig()
        self.fsUtil = FsUtilDelegate(self.toolsConfig)
        if os.path.exists(self.workDir):
            shutil.rmtree(self.workDir)
        os.mkdir(self.workDir)

    def testCheckOptions(self):
        cluster = ['-n', 'simple']
        config = ['-c', 'config']
        jobtype = ['-j', 'hadoop']
        buildmode = ['-m', 'full']
        data = ['-d', 'data']
        optionsCombine = [ cluster, config, jobtype, buildmode, data ]
        for i in xrange(len(optionsCombine)):
            try:
                testOptions = reduce(lambda a,b: a+b, options[:i] + options[i+1:])
                cmd = StartJobCmd()
                cmd.parseParams(testOptions)
            except Exception, e:
                pass
            else:
                self.assertTrue(False)

        config[1] = self.configPath
        options = reduce(lambda a,b: a+b, optionsCombine)

        # default
        self.cmd = StartJobCmd()
        setattr(self.cmd, 'initMember', self.__doNothing)
        self.cmd.parseParams(options)
        self.assertEqual('both', self.cmd.step)

        # inc merge
        buildmode[1] = 'incremental'
        options = reduce(lambda a,b: a+b, optionsCombine)

        self.cmd = StartJobCmd()
        setattr(self.cmd, 'initMember', self.__doNothing)
        self.cmd.parseParams(options + ['--step', 'both'])
        self.assertEqual('build', self.cmd.step)

        try:
            self.cmd = StartJobCmd()
            setattr(self.cmd, 'initMember', self.__doNothing)
            self.cmd.parseParams(options + ['--step', 'merge'])
        except:
            pass
        else:
            self.assertTrue(False)

        try:
            self.cmd = StartJobCmd()
            setattr(self.cmd, 'initMember', self.__doNothing)
            self.cmd.parseParams(options + ['--step', 'end_merge'])
        except:
            pass
        else:
            self.assertTrue(False)

    def __doNothing(self, options):
        self.cmd.step = options.step

if __name__ == "__main__":
    unittest.main()
