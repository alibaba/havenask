#!/bin/env python

class FakeProcess(object):
     def __init__(self, resultDict):
         assert(resultDict)
         self.resultDict = resultDict

     def run(self, cmd, noRetryExitCodeList = []):
         if self.resultDict.has_key(cmd):
             return self.resultDict[cmd]
         raise "Unexpected cmd[%s]" % cmd
