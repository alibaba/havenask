import re
from process import *

class PmRun(object):
    def __init__(self, pmRunExe, nuwaAddress):
        self.cmd = "%(pm_run)s -pm %(nuwaAddress)s" % {"pm_run": pmRunExe,
                                                       "nuwaAddress" : nuwaAddress}
        self.process = Process()

    def addPackage(self, packageName, localPackage):
        cmd = self.cmd + " AddPackage %(packageName)s meta %(localPackage)s" \
            % {"packageName": packageName, "localPackage": localPackage}
        self._runCmd(cmd)

    def removePackage(self, packageName):
        cmd = self.cmd + " RemovePackage %(packageName)s" \
            % {"packageName": packageName}
        self._runCmd(cmd)

    def listPackage(self):
        ## return stdout, stderr, retCode
        cmd = self.cmd + " GetPackageList"
        return self.process.run(cmd)

    def _runCmd(self, cmd):
        out, error, code = self.process.run(cmd)
        if code != 0:
            raise Exception('failed to run cmd[%s], stderr[%s], stdout[%s]' \
                            % (cmd, error, out))
        self._parseOutput(out)

    def _parseOutput(self, output):
        m = re.search(r"reply: \[([-]*[0-9]+)\]", output)
        if m is None:
            raise Exception( "failed to parse result from pm_run. output msg[%s]" % output)
            return False, error
        retCode = int(m.group(1))
        m = re.search(r"error: \[(.*)\]", output)
        if m is None:
            raise Exception( "failed to parse result from pm_run. output msg[%s]" % output)
        error = m.group(1)
        if retCode != 0:
            raise Exception(error)
