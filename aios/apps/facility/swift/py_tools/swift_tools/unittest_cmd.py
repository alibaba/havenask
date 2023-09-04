#!/bin/env python

import base_cmd
import process
import os
import sys
import tools_conf

class UnittestCmd(base_cmd.BaseCmd):
    def __init__(self, toolsConfFileDir):
        self.toolsConfFileDir = toolsConfFileDir

    def appendPath(self, envPath, path):
        if os.path.exists(path):
            return envPath + path + ":"

    def run(self):
        envPath = "PYTHONPATH="
        import tools_conf 
        pythonPath = []
        if hasattr(tools_conf, "python_path"):
            for path in tools_conf.python_path:
                if os.path.exists(path):
                    pythonPath.append(path.strip())
        
        installPythonPath = os.path.join(tools_conf.install_prefix, 
                                       "lib/python/site-packages/")
        if os.path.exists(installPythonPath):
            pythonPath.append(installPythonPath)                          
        pythonPath.append(self.toolsConfFileDir)

        envPath = "%s%s" % (envPath, ":".join(pythonPath))

        curPath = os.path.split(os.path.realpath(__file__))[0]
        testMainPath = curPath + '/test/test_main.py'
        cmd = envPath + ' /bin/env python ' + testMainPath
        processor = process.Process()
        data, error, code = processor.run(cmd)
        print error
        return data, error, code

