#!/bin/env python
import os, sys
import subprocess

class Process(object):
    def run(self, cmd):
        p = subprocess.Popen(cmd, shell=True, close_fds=False, 
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = p.communicate()
        return stdout, stderr, p.returncode

if __name__ == '__main__':
    process = Process()
    print process.run(sys.argv[1])
