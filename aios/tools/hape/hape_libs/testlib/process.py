# process.py
"""        Wrapper for process control in python
@version:  $Id$
@author:   U{Jackie Jiang<mailto:zijun.jiangzj@aliyun-inc.com>}
@see:      http://grd.alibaba-inc.com/projects/heavenasks/wiki/HA3_Auto_Testing
"""

import time
import signal
import re
import os
import tempfile
import sys
import shlex
import subprocess
import traceback
import string
import logging

log = logging.getLogger("hape")
# log.warning("Deprecated: Use of 'common_utils.process' is deprecated.")
# log.warning("Please use 'atest.auto' instead.")

class Process:
    GlobalTimout = 0
    intervalSecond = 1

    def run(self, cmd, timeout = None):
        if timeout == None:
            timeout = 0
        return self.runWithTimeOut(cmd, timeout)
    
    def runWithTimeOut(self, cmd, timeout = GlobalTimout):
        ''' Usage:             fork a subprocess and run (foreground)
            Keyword arguments: cmd
            Returns:           data, error, code
        '''
        timeLeft = timeout
        beforeRun = time.time()
        startTime = self._time2str(beforeRun)
        log.info("run cmd start: [\n%s\n] start time: %s, timeout: %s"%(repr(cmd), startTime, timeLeft))
        env_table = os.environ
        if "LD_LIBRARY_PATH" in os.environ:
            env_table["LD_LIBRARY_PATH"] = os.environ["LD_LIBRARY_PATH"]
        fdout = tempfile.TemporaryFile()
        fderr = tempfile.TemporaryFile()
        p = subprocess.Popen(cmd.encode('utf8'), shell=True, close_fds=False, stdout=fdout,  \
                                  stderr=fderr, env=env_table)            
        if timeLeft <= 0:
            #set to be waiting endless
            timeLeft = -1
        while timeLeft > 0:
            if not p.poll() == None:
                break
            time.sleep(self.intervalSecond)
            timeLeft = timeLeft - self.intervalSecond
            
        if p.poll() == None and timeLeft >= 0:
            self.__killAll(p.pid)
            log.info('Timeout: %s seconds, kill the process [%s]' % (timeLeft, cmd[:1000]))
        stdout,stderr = p.communicate()
        fdout.seek(0)
        fderr.seek(0)
        stdout = fdout.read()
        stderr = fderr.read()
        fdout.close()
        fderr.close()

        afterRun = time.time()
        endTime = self._time2str(afterRun)
        log.info("run cmd finish, end time: %s, time used: %s"%(endTime, (afterRun - beforeRun)))
        return (stdout, stderr, p.returncode)

    def consoleRun(self, cmd, timeout = GlobalTimout):
        ''' Usage:             fork a subprocess and run (foreground)
            Keyword arguments: cmd
            Returns:           code
        '''
        args = shlex.split(cmd)
        p = subprocess.Popen(args)
        if timeout <= 0:
            #set to be waiting endless
            timeout = -1
        while timeout > 0:
            if not p.poll() == None:
                break
            time.sleep(intervalSecond)
            timeout = timeout - 1
        if p.poll() == None and timeout >= 0:
            self.__killAll(p.pid)
        p.communicate()
        return p.returncode

    def expressRun(self, cmd):
        ''' Usage:             fork a subprocess and run (foreground or backgournd)
            Keyword arguments: cmd
            Returns:           str
        '''
        str = os.popen(cmd).read()    
        return str.strip()

    def runWrapper(self, cmd, timeout = GlobalTimout):
        ''' Usage:             fork a subprocess and run (foreground)
                               also print the returns
            Keyword arguments: cmd
            Returns:           data, error, code
        '''
        data, error, code = self.run(cmd, timeout)
        print(data)
        print(error)
        print(code)
        return data, error, code

    def remoteRun(self, user, host, cmd, timeout="600"):
        ''' Usage:             ssh                               
            Keyword arguments: user, host, cmd
            Returns:           data, error, code
        '''
        cmd = ['ssh', user + '@' + host, '-o ConnectTimeout=' + timeout, cmd]
        log.info(' '.join(cmd))
        p = subprocess.Popen(cmd, stdout = subprocess.PIPE,\
                            stderr = subprocess.PIPE, shell = False, close_fds = True)
        stdout,stderr = p.communicate()
        return (stdout, stderr, p.returncode)

    def remoteRunNoWait(self, user, host, cmd, timeout="600", output=None):
        ''' Usage:             ssh                               
            Keyword arguments: user, host, cmd
            Returns:           data, error, code
        '''
        if output is None:
            out = subprocess.PIPE
        else:
            out = open(output, 'w')
        cmd = ['ssh', user + '@' + host, '-o ConnectTimeout=' + timeout, cmd]
        log.info("cmd: %s" % cmd)
        p = subprocess.Popen(cmd, stdout=out, stderr=subprocess.PIPE, 
                             shell=False, close_fds=True)
        log.info(' '.join(cmd))
        return p.pid

    def remoteRunSudo(self, user, password, host, cmd, timeout=30, logname=None):
        cmd = 'sudo ' + cmd
        self.remoteRunExpect(user, password, host, cmd, timeout=timeout,
                             logname=logname)

    def remoteCopy(self, source, target, timeout="600"):
        ''' Usage:             scp                               
            Keyword arguments: source, target
            Returns:           data, error, code
        '''
        cmd = ['scp','-o ConnectTimeout=' + timeout, '-r', source, target]
        p = subprocess.Popen(cmd, stdout = subprocess.PIPE,\
                            stderr = subprocess.PIPE, shell = False, close_fds = True)
        stdout,stderr = p.communicate()
        log.info(' '.join(cmd))
        return (stdout, stderr, p.returncode)

    def __killAll(self, tokillpid, killsignal = signal.SIGKILL):
        tokillpid = str(tokillpid)
        pids = dict()
        p = re.compile(r'\s+')

        lines = os.popen('ps -ef').read().strip().split('\n')

        for line in lines[1:]:
            line = p.split(line.lstrip())
            pid = line[1]
            ppid = line[2]
            if pid == ppid:
                continue
            if(ppid in pids):
                pids[ppid].append(pid)
            else:
                pids[ppid] = [pid]

        tokill = [tokillpid]
        loop = 0
        length = len(tokill)
        while loop < length:
            pid = tokill[loop]
            if pid in pids:
                tokill.extend(pids[pid])
                length = len(tokill)
            try:
                os.kill(int(pid), killsignal)
            except OSError:
                log.info('Subprocess %s has already been killed' % pid)
            loop = loop+1

    def _time2str(self, value):
        floatValue = float(value)
        if floatValue == -1:
            return value
        return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(floatValue))