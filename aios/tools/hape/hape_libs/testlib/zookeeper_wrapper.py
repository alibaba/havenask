#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
from contextlib import closing
import socket
import hape_libs.testlib.process as process

def _get_local_host():
    try:
        # 获取本机的主机名
        hostname = socket.gethostname()
        # 使用主机名获取本机的 IPv4 地址
        local_ip = socket.gethostbyname(hostname)
    except socket.error:
        # 如果获取失败，返回默认地址
        local_ip = "127.0.0.1"
    return local_ip

class ZookeeperWrapper:
    def __init__(self, zookeeper_bin_dir,  port, tmp_dir = None):
        self.zookeeperBinDir = zookeeper_bin_dir
        self.zookeeperProcess = process.Process()
        self.tmp_dir = tmp_dir
        start_shell = os.path.join(self.zookeeperBinDir, "start.sh")
        stop_shell = os.path.join(self.zookeeperBinDir, "stopall.sh")
        stop_not_del_shell = os.path.join(self.zookeeperBinDir, "stopallnotdeldata.sh")
        exportEnv = ""
        if tmp_dir:
            exportEnv = "export TEST_TMPDIR=" + self.tmp_dir +"; "
        self.zookeeperStartCmd = exportEnv + " %s %s %s" % (start_shell, port, port)
        self.zookeeperStopCmd = exportEnv + stop_shell
        self.zookeeperStopCmdNotDelData = exportEnv + stop_not_del_shell
        self.zookeeperStopByPortCmd = exportEnv + os.path.join(self.zookeeperBinDir, "stop.sh")
        # with closing(os.popen("/sbin/ifconfig | grep 'inet' | awk '{print $2}'")) as s:
        #     ip = s.read()
        #     self.ip = ip[ip.find(':')+1:ip.find('\n')]
        self.ip = _get_local_host()
        
        self.port = port
        self.address = "zfs://" + self.ip + ":" + str(self.port)

    def getAddress(self):
        return self.address

    def start(self):
        cwd = os.getcwd()
        os.chdir(self.zookeeperBinDir)
        stdout, stderr, returnCode = self.zookeeperProcess.run(self.zookeeperStartCmd)
        os.chdir(cwd)
        return returnCode

    def stopAll(self, clear=False):
        if clear:
            cmd = self.zookeeperStopCmd + " 1"
        else:
            cmd = self.zookeeperStopCmd + " 0"
        cwd = os.getcwd()
        os.chdir(self.zookeeperBinDir)
        stdout, stderr, returnCode = self.zookeeperProcess.run(cmd)
        os.chdir(cwd)
        return returnCode

    def stop(self):
        cmd = '%s %d' % (self.zookeeperStopCmdNotDelData, self.port)
        cwd = os.getcwd()
        os.chdir(self.zookeeperBinDir)
        stdout, stderr, returnCode = self.zookeeperProcess.run(cmd)
        os.chdir(cwd)
        return returnCode

    def stopNotDelData(self):
        cmd = '%s %d' % (self.zookeeperStopCmdNotDelData, self.port)
        cwd = os.getcwd()
        os.chdir(self.zookeeperBinDir)
        stdout, stderr, returnCode = self.zookeeperProcess.run(cmd)
        os.chdir(cwd)
        return returnCode

    def getPidByPort(self, port):
        pidFile = '%s/var/log/zookeeper.pid.%d' % (self.zookeeperBinDir, port)
        f = open(pidFile, 'r')
        pid = f.read()
        f.close()
        return int(pid)

if __name__ == '__main__':
    zookeeper = ZookeeperWrapper('/home/hongjs/galaxy/online/swift_autotest/swift_systemtest/swift_func_test/grp_failover/zookeeper',"./", 22345)
    print('start begin')
    zookeeper.start()
    print('start end')
    import time
    time.sleep(1)
    print('stop begin')
    zookeeper.stop()
    print('stop end')
