# -*- coding: utf-8 -*-

import job_delegate
from common_define import *
import process

class LocalJobDelegate(job_delegate.JobDelegate):
    def __init__(self, configPath, buildAppConf, buildRuleConf,
                 toolsConfig, fsUtil, clusterName, tabletMode=False):
        super(LocalJobDelegate, self).__init__(configPath, buildAppConf,
                                               buildRuleConf, toolsConfig,
                                               fsUtil, clusterName, tabletMode)

    def doStartJob(self, step, mapCount, reduceCount):
        logConfigPath = self.logConfigPath
        if logConfigPath:
            logConfigPath = " -l %s" % logConfigPath
        cmd = "%s -s %s -m %s -r %s -p '%s' %s  2>> bs_stderr.out" % (self.toolsConfig.getLocalJob(),
                                                                                 self.__toStepString(step),
                                                                                 mapCount, reduceCount, self.generateJobParamStr(),
                                                                                 logConfigPath)
        p = process.Process()
        print cmd
        data, error, code = p.run(cmd)
        if code != 0:
            if cmd.find('ASAN_OPTIONS') >= 0:
                print "use asan, asan detect has error"
            else:    
                raise Exception("local build failed data[%s] error[%s] code[%s]"
                                % (data, error, str(code)))
        return True

    def __toStepString(self, step):
        if step == JOB_BUILD_STEP:
            return 'build'
        elif step == JOB_MERGE_STEP:
            return 'merge'
        elif step == JOB_END_MERGE_STEP:
            return 'endmerge'
        else:
            assert(False)

    def stopJob(self):
        return True

    def isJobRunning(self):
        return JOB_STATUS_NOT_RUNNING

    def doGetJobStatus(self, showDetail, step, timeout):
        return True, JOB_STATUS_SUCCESS

    def waitJobFinish(self, timeout, step, sleepInterval):
        pass
