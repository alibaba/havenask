#!/bin/env python
import os, tempfile, sys, time
import subprocess

class Process(object):
     def __init__(self, retryDuration = 0, retryInterval = 0, retryTimes = 1):
         self.retryDuration = retryDuration
         self.retryInterval = retryInterval
         self.retryTimes = retryTimes
 
     def run(self, cmd, noRetryExitCodeList = []):
         # check interval > 0
         if self.retryInterval <= 0:
             return self._doRun(cmd)
 
         # code = 0 is ok
         noRetryExitCodeList.append(0)
         while True:
             begin = time.time()
             out, err, code = self._doRun(cmd)
             end = time.time()
             if code in noRetryExitCodeList:
                 break

             self.retryTimes -= 1
             self.retryDuration -= ((end - begin) + self.retryInterval)
             if self.retryDuration <= 0 and self.retryTimes < 1:
                 break
             # first retryDuration--, save one sleep time
             time.sleep(self.retryInterval)
 
         return out, err, code
 
     def _doRun(self, cmd):
         p = subprocess.Popen(cmd, shell=True, close_fds=False, 
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
         stdout, stderr = p.communicate()
         return stdout, stderr, p.returncode

     def runInBackground2(self, cmd, outFileName, errFileName):
         env_table = os.environ
         if "LD_LIBRARY_PATH" in os.environ:
             env_table["LD_LIBRARY_PATH"] = os.environ["LD_LIBRARY_PATH"]
         sameFile = False
         fout = open(outFileName, 'w')
         if errFileName == outFileName:
             ferr = fout
             sameFile = True
         else:
             ferr = open(errFileName, 'w')
         p = subprocess.Popen(cmd, shell=True, close_fds=False, stdout=fout, 
                              stderr=ferr, env=env_table)
         fout.close()
         if not sameFile:
             ferr.close()
         return p

     def runinConsole(self, cmd):
         cmd = '%(cmd)s 1>>%(out)s 2>>%(err)s' %\
             {'cmd': cmd,\
                  'out': '/dev/null',\
                  'err': '/dev/null'}
         returnCode = os.system(cmd)
         return returnCode
