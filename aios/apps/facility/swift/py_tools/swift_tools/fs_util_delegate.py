#!/bin/env python
from process import *
import string
import re


class FsUtilDelegate(object):
    def __init__(self, fsUtilExe, libPath):
        self.exe = fsUtilExe
        self.libPath = libPath
        self.process = Process()
        self.__initCmd()

    def __initCmd(self):
        if (self.libPath is None or self.libPath == ""):
            self.envCmd = ""
        else:
            self.envCmd = "/bin/env LD_LIBRARY_PATH=%(libPath)s " % \
                {'libPath': self.libPath}
        self.cmd = self.envCmd + self.exe + ' '

    def addHadoopHome(self, hadoopHome):
        if hadoopHome is not None:
            self.cmd = self.envCmd + " HADOOP_HOME=" + hadoopHome + " " + self.exe + ' '
        else:
            print "Hodoop Home is None"

    # Return directory list if path exists, return None if path does not exist;
    # otherwise raise exception
    def listDir(self, path, sepStr=None):
        if not self.exists(path):
            return None
        cmd = "ls '" + path + "'"
        out, error, code = self.process.run(self.cmd + cmd)
        if code != 0:
            errorMsg = "ERROR: error message[" + error + "], error code[" + str(code) + "]" + "], out[" + str(out) + "]"
            raise Exception, errorMsg
        return out.split(sepStr)

    # Return true if path is directory, return false if path does not exist or path is file;
    # otherwise raise exception
    def isDir(self, path):
        if not self.exists(path):
            return False
        cmd = "ls '" + path + "'"
        out, error, code = self.process.run(self.cmd + cmd)
        if code == 11:  # path is not directory
            return False
        elif code != 0:
            errorMsg = "ERROR: error message[" + error + "], error code[" + str(code) + "]" + "], out[" + str(out) + "]"
            raise Exception, errorMsg
        return True

    # Return true if remove path success, return false if path does not exist;
    # otherwise raise exception
    def remove(self, path):
        cmd = "rm '" + path + "'"
        out, error, code = self.process.run(self.cmd + cmd)
        if code == 10:  # path does not exist
            return False
        elif code != 0:
            errorMsg = "ERROR: error message[" + error + "], error code[" + str(code) + "]" + "], out[" + str(out) + "]"
            raise Exception, errorMsg
        return True

    # will not raise exception
    def copy(self, srcPath, destPath, overwrite=False):
        try:
            if self.exists(destPath) and overwrite:
                if not self.remove(destPath):
                    err = "ERROR: failed to copyDir %s to %s, error[cann't remove existing dir %s]" % (
                        srcPath, destPath, destPath)
                    print err
                    return False
        except Exception as exceptInfo:
            print exceptInfo
            return False

        cpCmd = "cp -r '" + srcPath + "' '" + destPath + "'"
        out, error, code = self.process.run(self.cmd + cpCmd)
        if code != 0:
            errorMsg = "copy from [" + srcPath + "] to [" + destPath  \
                + " ] failed! error message[" + error + "], error code[" + str(code) + "]" + "], out[" + str(out) + "]"
            print errorMsg
            return False
        return True

    # not raise exception
    def cat(self, filePath):
        catCmd = "cat '" + filePath + "'"
        (stdout, stderr, returncode) = self.process.run(self.cmd + catCmd)
        if returncode == 0:
            return stdout.rsplit('\n', 1)[0]
        else:
            print "ERROR: cat file [%s] failed, %s %s" % (filePath, stdout, stderr)
            return None

    # return True if path exists, return False if path does not exists,
    # otherwise raise an exception
    def exists(self, path):
        cmd = "isexist '" + path + "'"
        out, error, code = self.process.run(self.cmd + cmd)
        if code == 7:  # path does not exist
            return False
        if code == 16:  # path does exist
            return True

        print "ERROR: failed to check exists, path:", path
        errorMsg = "ERROR: error message[" + error + "], error code[" + str(code) + "], out[" + str(out) + "]"
        raise Exception, errorMsg

    # not raise exception
    def mkdir(self, path, recursive=True):
        if recursive:
            cmd = "mkdir -p '" + path + "'"
        else:
            cmd = "mkdir '" + path + "'"

        out, error, code = self.process.run(self.cmd + cmd)
        if code != 0:
            return False
        return True

    def getMeta(self, path):
        cmd = "getmeta '" + path + "'"
        out, error, code = self.process.run(self.cmd + cmd)
        if code != 0:
            errorMsg = "ERROR: error message[" + error + "], error code[" + str(code) + "]" + "], out[" + str(out) + "]"
            raise Exception, errorMsg
        else:
            return out

    def rename(self, srcPath, destPath):
        renameCmd = "rename  '" + srcPath + "' '" + destPath + "'"
        data, error, code = self.process.run(self.cmd + renameCmd)
        if code != 0:
            raise Exception(
                "rename [%s] to [%s] failed : data[%s], error[%s], code[%d]" %
                (srcPath, destPath, data, error, code))
        return True

    def mv(self, srcPath, destPath):
        renameCmd = "mv  '" + srcPath + "' '" + destPath + "'"
        data, error, code = self.process.run(self.cmd + renameCmd)
        if code != 0:
            raise Exception(
                "mv [%s] to [%s] failed : data[%s], error[%s], code[%d]" %
                (srcPath, destPath, data, error, code))
        return True

    def normalizeDir(self, dirName):
        if dirName and not dirName.endswith('/'):
            return dirName + '/'
        return dirName

    def getMaxConfigVersion(self, path):
        maxVersion = -1
        versions = self.listDir(path, '\n')
        if versions is None:
            # print "ERROR: failed to list path [%s]" % path
            return maxVersion

        if versions == ['']:
            return maxVersion

        for i in versions:
            if len(i) > 0 and i.isdigit():
                version = string.atoi(i)
                if version > maxVersion:
                    maxVersion = version
        return maxVersion
