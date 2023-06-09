#!/bin/env python
from process import *
from common_define import *

class HadoopJobParam:
    files = ""
    jobConfFile = ""
    inputDir = ""
    outputDir = ""
    parameters = ""
    
class HadoopUtil(object):
    def __init__(self, hadoopHome):
       self.process = Process()
       self.hadoopTool = '%s/bin/hadoop' % hadoopHome
       self.yarnClient = '%s/bin/yarn' % hadoopHome

    # used by mapreduce 1.0    
    def getJobHistory(self, outputDir):
        cmd = "%(hadoopTool)s job -history %(outputDir)s " % {
            'hadoopTool': self.hadoopTool,
            'outputDir': outputDir}
        out, err ,code = self.process.run(cmd)
        if code != 0:
            print "ERROR: faild to get history info for %s " % outputDir
            print "errInfo: ", err
            return None
        return out

    # used by mapreduce 2.0    
    def getJobStatus(self, jobId):
        cmd = "%(hadoopTool)s job -status %(jobId)s" % {
            'hadoopTool': self.hadoopTool,
            'jobId': jobId}
        out, err ,code = self.process.run(cmd)
        if code != 0:
            errorMsg = "ERROR: get job status, ErrorMsg [%s]" % err
            raise Exception, errorMsg
        return out

    # for yarn
    def getApplicationInfo(self, jobId):
        '''
        jobId format: job_%d_%d
        '''
        assert len(jobId) > 4, "invalid jobId[%s]" % jobId
        appId = "application_" + jobId[4:]
        cmd = "%(yarnClient)s application -status %(appId)s" % {
            'yarnClient': self.yarnClient,
            'appId': appId}
        out, err ,code = self.process.run(cmd)
        if code != 0:
            errorMsg = "ERROR: get application status, ErrorMsg [%s]" % err
            raise Exception, errorMsg
        # parse output
        lines = out.split('\n')
        results = []
        jobStatus = 'Unknown'
        for line in lines:
            if 'Application-Id' in line:
                results.append(line.strip())
            if 'Application-Name' in line:
                results.append(line.strip())
            if 'Start-Time' in line:
                results.append(line.strip())
            if 'Finish-Time' in line:
                results.append(line.strip())
            if 'Final-State' in line:
                line = line.strip()
                results.append(line)
                status = line.split(':')[1].strip()
                jobStatus = status
        return jobStatus, results
    
    def runPipes(self, jobConfFile, inputDir, outputDir, 
                 pipesOutFileName, inputFormat, jobPackage, parameters):
        cmd = "%(hadoopTool)s pipes " % {
            'hadoopTool': self.hadoopTool}
        cmd += " -archives " + jobPackage
        cmd += " -conf %(jobConfFile)s " \
            "-Dmapred.line.input.format.linespermap=1 " \
            "%(parameters)s " \
            "-input %(inputDir)s " \
            "-output %(outputDir)s " \
            % {'jobConfFile': jobConfFile,
               'inputDir' : inputDir,
               'outputDir' : outputDir,
               'parameters' : parameters}
        if inputFormat:
            cmd += " -inputformat " + inputFormat

        p = self.process.runInBackground2(cmd, pipesOutFileName, 
                                          pipesOutFileName)
        return p
            
    def getJobList(self):
        cmd = """%(hadoopTool)s job -list""" % {'hadoopTool': self.hadoopTool}
        out, err, code = self.process.run(cmd)
        if (code != 0):
            errorMsg = "ERROR: get job list, ErrorMsg [%s]" % err
            raise Exception, errorMsg
        
        # parse output
        lines = out.splitlines()
        if len(lines) < 2:
            errorMsg = "ERROR: invalid output of get job list, out:[%s]" % out
            raise Exception, errorMsg

        # skip the first two lines
        jobListLine = lines[2:]
        jobList = [line.split()[0] for line in jobListLine]
        return jobList

    def killJob(self, jobId):
        cmd = """%(hadoopTool)s job -kill %(jobId)s""" % {
            'hadoopTool': self.hadoopTool,
            'jobId': jobId}
        out, err ,code = self.process.run(cmd)
        if code != 0:
            print "ERROR: faild to stop job [%s]" % jobId
            print "errInfo: ", err
            return False
        return True

    def getStatusFromJobTracker(self, jobId):
        cmd = """%(hadoopTool)s job -status %(jobId)s""" % {
            'hadoopTool': self.hadoopTool,
            'jobId': jobId}
        return self.process.run(cmd)

    def normalizeDir(self, dirName):
        if dirName and not dirName.endswith('/'):
            return dirName + '/'
        return dirName

if __name__ == '__main__':
    hadoopHome = '/home/yangkt/hadoop-2.0.0-cdh4.3.0'
    hadoopUtil = HadoopUtil(hadoopHome)
    print hadoopUtil.getJobStatus("job_201307241700_0249")
