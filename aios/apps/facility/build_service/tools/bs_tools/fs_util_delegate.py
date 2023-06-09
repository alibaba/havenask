from process import *
import string
import re

class FsUtilDelegate(object):
    def __init__(self, toolsConf):
        self.cmd = toolsConf.getFsUtil()
        self.hadoopUser = toolsConf.hadoop_user
        self.hadoopGroup = toolsConf.hadoop_group
        self.hadoopReplica = toolsConf.hadoop_replica
        self.process = Process(toolsConf.retry_duration, toolsConf.retry_interval, toolsConf.retry_times)

    def getFilePathWithFsConfigStr(self, oriPath):
        oriPath = oriPath.strip()
        if oriPath.startswith("hdfs://"):
            return self.adaptHadoopPath(oriPath, self.hadoopUser, self.hadoopGroup,
                                        self.hadoopReplica)
        else:
            return oriPath

    def adaptHadoopPath(self, path, hadoopUserName, hadoopGroupName, hadoopReplica):
        authIndex = path.find('?', 7)
        pathIndex = path.find('/', 7)
        auth = ""
        if authIndex == -1:
            authIndex = pathIndex
        else:
            auth = path[authIndex:pathIndex]
        auth = self.doAdaptHadoopPath(auth, "user", hadoopUserName)
        auth = self.doAdaptHadoopPath(auth, "groups", hadoopGroupName)
        auth = self.doAdaptHadoopPath(auth, "rep", hadoopReplica)
        auth = '?' + auth[1:] # replace first &/? by ?
        path = ''.join((path[:authIndex], auth, path[pathIndex:]))
        return path

    def doAdaptHadoopPath(self, auth, typeName, typeValue):
        if auth.find(typeName + "=") == -1 and typeValue != None and typeValue != '':
            auth += '&%s=%s' % (typeName, typeValue)
        return auth

    #Return directory list if path exists, return None if path does not exist;
    #otherwise raise exception
    def listDir(self, path, sepStr = None):
        path = self.getFilePathWithFsConfigStr(path)
        cmd = "ls '" + path + "'"
        data, error, code = self.process.run(self.cmd + cmd)
        if code != 0:
            raise Exception("list dir[%s] failed : data[%s], error[%s], code[%d]" % (path, data, error, code))
        return data.split(sepStr)

    #Return true if path is directory, return false if path does not exist or path is file;
    #otherwise raise exception
    def isDir(self, path):
        path = self.getFilePathWithFsConfigStr(path)
        cmd = "ls '" + path + "'"
        data, error, code = self.process.run(self.cmd + cmd, [11])
        if code == 11: #path is not directory
            return False
        elif code != 0:
            raise Exception("isDir [%s] failed : data[%s], error[%s], code[%d]" % (path, data, error, code))
        return True

    #Return true if remove path success, return false if path does not exist;
    #otherwise raise exception
    def remove(self, path):
        path = self.getFilePathWithFsConfigStr(path)
        cmd = "rm '" + path + "'"
        data, error, code = self.process.run(self.cmd + cmd, [10])
        if code != 0:
            raise Exception("remove [%s] failed : data[%s], error[%s], code[%d]" % (path, data, error, code))

    #will not raise exception
    def copy(self, srcPath, destPath):
        newDestPath = self.getFilePathWithFsConfigStr(destPath)
        srcPath = self.getFilePathWithFsConfigStr(srcPath)
        cpCmd = "cp -r '" + srcPath + "' '" + newDestPath + "'"
        data, error, code = self.process.run(self.cmd + cpCmd)
        if code != 0:
            raise Exception("copy [%s] to [%s] failed : data[%s], error[%s], code[%d]" % (srcPath, destPath, data, error, code))

    #will not raise exception
    def rename(self, srcPath, destPath):
        newDestPath = self.getFilePathWithFsConfigStr(destPath)
        srcPath = self.getFilePathWithFsConfigStr(srcPath)

        renameCmd = "rename  '" + srcPath + "' '" + newDestPath + "'"
        data, error, code = self.process.run(self.cmd + renameCmd)
        if code != 0:
            raise Exception("rename [%s] to [%s] failed : data[%s], error[%s], code[%d]" % (srcPath, destPath, data, error, code))

    #raise exception if read failed.
    def cat(self, filePath):
        filePath = self.getFilePathWithFsConfigStr(filePath)
        catCmd = "cat '" + filePath + "'"
        data, error, code = self.process.run(self.cmd + catCmd + ' -l /dev/null')
        if code == 0:
            if len(data) > 0:
                if data[-1] == '\n':
                    data = data[:-1]
            return data
        else:
            raise Exception("cat file[%s] failed : data[%s], error[%s], code[%d]" % (filePath, data, error, code))

    #return True if path exists, return False if path does not exists,
    #otherwise raise an exception
    def exists(self, path):
        path = self.getFilePathWithFsConfigStr(path)
        cmd = "isexist '" + path + "'"
        data, error, code = self.process.run(self.cmd + cmd, [7, 16])
        if code == 7: #path does not exist
            return False
        if code == 16: #path does exist
            return True
        raise Exception("check exist[%s] failed : data[%s], error[%s], code[%d]" % (path, data, error, code))

    #not raise exception
    def mkdir(self, path, recursive = True):
        path = self.getFilePathWithFsConfigStr(path)
        if recursive:
            cmd = "mkdir -p '" + path + "'"
        else:
            cmd = "mkdir '" + path + "'"

        data, error, code = self.process.run(self.cmd + cmd)

        if code != 0:
            raise Exception("mkdir[%s] failed : data[%s], error[%s], code[%d]" % (path, data, error, code))

    def normalizeDir(self, dirName):
        if dirName and not dirName.endswith('/'):
            return dirName + '/'
        return dirName

    def getMaxConfigVersion(self, path):
        maxVersion = -1
        versions = self.listDir(path, '\n')
        if versions == None :
            #print "ERROR: failed to list path [%s]" % path
            return maxVersion

        if versions == ['']:
            return maxVersion

        for i in versions:
            if len(i) > 0 and i.isdigit():
                version = string.atoi(i)
                if version > maxVersion:
                    maxVersion = version
        return maxVersion
