#!/bin/env python
#import local_file_util
import fs_util_delegate

#class FakeFileUtil(local_file_util.LocalFileUtil, fs_util_delegate.FsUtilDelegate):
class FakeFileUtil(fs_util_delegate.FsUtilDelegate):
    fakeContent = ""
    fakeDirList = []
    notExistPath = []
    cmdExistExcept = False

    def __init__(self, toolsConfig=None):
        pass

    def setFakeContent(self, content):
        self.fakeContent = content

    def setFakeDirList(self, dirList):
        self.fakeDirList = dirList
            
    def cat(self, path):
        return self.fakeContent

    def listDir(self, path, outputError=True):
        return self.fakeDirList
    
    def copyDir(self, srcDir, distDir):
        return True
    
    def panguFileExist(self, filePath):
        return False

    def dirExist(self, dirPath):
        return True

    def setNotExistPath(self, filePath):
        self.notExistPath.append(filePath)

    def exists(self, filePath):
        if self.cmdExistExcept:
            raise Exception, "exception"
        if filePath in self.notExistPath:
            return False
        return True

    def isDir(self, path):
        return True

    def remove(self, path):
        return True

    def getMaxConfigVersion(self, path):
        return 0

    def mkdir(self, path, recursive = True):
        return True

    def copy(self, srcPath, destPath, overwrite = False):
        return True

    def rename(self, srcPath, destPath):
        return True

    def setHadoopEnv(self):
        pass
