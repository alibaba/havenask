#!/bin/env python

import re
import sys
import os
import socket
from optparse import OptionParser
import tempfile
import subprocess
import json
import commands
import time
import local_search_starter

class LocalSearchStopCmd(local_search_starter.LocalSearchStartCmd):
    '''
local_search_stop.py
    {-c config_dir          | --config=config_dir}
options:
    -c config_dir,    --config=config_dir        : optional [default os.getcwd()]

examples:
    ./local_search_stop.py -c config_dir
    '''
    def __init__(self):
        self.parser = OptionParser(usage=self.__doc__)

    def usage(self):
        print self.__doc__ % {'prog' : sys.argv[0]}

    def addOptions(self):
        self.parser.add_option('-c', '--config', action='store', dest='configPath', default="")

    def parseParams(self, optionList):
        self.optionList = optionList
        self.addOptions()
        (options, args) = self.parser.parse_args(optionList)
        self.options = options
        self.initMember(options)
        return True

    def initMember(self, options):
        self.configPath = options.configPath
        if self.configPath == "":
            self.configPath = os.getcwd()

    def run(self):
        cmd = "ps xfww  | grep sap_server_d | grep '%s'| grep -v grep | awk {'print $1'}" % self.configPath
        print cmd
        (ret, result) = commands.getstatusoutput(cmd)
        if ret != 0:
            print "can not find process"
            return "", "", 1
        for pid in result.splitlines():
            if pid != "":
                print "kill %s" % str(pid)
                os.system("kill %s" % str(pid))
        time.sleep(1)
        cmd = "ps xfww  | grep sap_server_d | grep '%s'| grep -v grep | awk {'print $1'}" % self.configPath
        print cmd
        (ret, result) = commands.getstatusoutput(cmd)
        if ret != 0:
            print "can not find process"
            return "", "", 1
        for pid in result.splitlines():
            if pid != "":
                print "kill -9 %s" % str(pid)
                os.system("kill -9 %s" % str(pid))

        return "", "", 0

    def _getPid(self, port):
        p = subprocess.Popen(['netstat','-plant'],stdout=subprocess.PIPE)
        out,err = p.communicate()
        pids = []
        port = str(port)
        for line in out.splitlines():
            if line.find("sap_server_d") != -1:
                if line.find(port) != -1:
                    parts = line.split(" ")
                    for p in parts:
                        if p.find("sap_server_d") != -1:
                            pid = p.split("/")[0]
                            return int(pid)
        return -1

if __name__ == '__main__':
    cmd = LocalSearchStopCmd()
    if not cmd.parseParams(sys.argv):
        cmd.usage()
        sys.exit(-1)
    data, error, code = cmd.run()
    if code != 0:
        if error:
            print error
        sys.exit(code)
    sys.exit(0)
