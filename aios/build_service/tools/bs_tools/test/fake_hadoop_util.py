#!/bin/env python

from hadoop_util import HadoopUtil
import os

class FakeHadoopUtil(HadoopUtil):
    jobList = []
    getJobListExcept = False

    def __init__(self, hadoopHome):
        self.content = ""
        self.jobHistory = None
#         self.jobHistory = '''
# Hadoop job: job_201112161819_0901
# '''

    def exists(self, dir):
        return True

    def makeDirOnHadoop(self, dir):
        return True

    def removeDirOnHadoop(self, dir):
        return True

    def copyFromLocal(self, localPath, hadoopPath):
        return True

    def runPipes(self, jobConfFile, inputDir, outputDir, 
                 pipesOutFileName, inputFormat, jobPackage=None, parameters=None):
        self.cmd_inputDir = inputDir
        self.cmd_inputFormat = inputFormat
        return True

    def setFileContent(self, content):
        self.content = content

    def catFile(self, fileName):
        return self.content

    def removeFile(self, fileName):
        return True

    def getJobList(self):
        if self.getJobListExcept:
            raise Exception, "get job list failed"

        self.jobList.append("job_201103101836_0026")
        self.jobList.append("job_201103101836_0025")
        return self.jobList

    def killJob(self, jobId):
        return True

    def getStatusFromJobTracker(self, jobId):
        return "", "", 0

    def listDir(self, path):
        return os.listdir(path)

    def setJobHistory(self, jobHistory):
        self.jobHistory = jobHistory

    def getJobHistory(self, outputDir):
        return self.jobHistory
        
    def getCmdInputFormat(self):
        return self.cmd_inputFormat

    def getCmdInputDir(self):
        return self.cmd_inputDir

    def getClassPathForJob(self, hadoopHome):
        return ''

    def getApplicationInfo(self, jobId):
        if jobId == 'job_success':
            return 'SUCCEEDED', ''
        if jobId == 'job_running':
            return 'RUNNING', ''
        if jobId == 'job_failed':
            return 'FAILED', ''
        if jobId == 'job_accept':
            return 'ACCEPTED', ''
        if jobId == 'job_killed':
            return 'KILLED', ''
        if jobId == 'job_unknown':
            return 'UNKNOWN', ''
