#!/usr/bin/python
import os
import socket
import local_file_util
import tempfile

CURPATH = os.path.split(os.path.realpath(__file__))[0]
AUTHORITY_FILE_NAME = 'authority_file' 
LOCAL_FILE_NAME = '_#_swift_authoriy_file_#_'
class SwiftAuthorityCheckor:
    def __init__(self, zkAddress, fileUtil):
        self.authorityFile = zkAddress + '/' + AUTHORITY_FILE_NAME
        self.fileUtil = fileUtil

    def validate(self):
        expect_key = self.get_host_ip() + "," + CURPATH
        if not self.fileUtil.exists(self.authorityFile):
            localFileUtil = local_file_util.LocalFileUtil()
            tmpFile = tempfile.NamedTemporaryFile()
            tmpFile.write(expect_key)
            tmpFile.flush()
            os.fsync(tmpFile.fileno())
            self.fileUtil.copy(tmpFile.name, self.authorityFile)
            tmpFile.close()
        content = self.fileUtil.cat(self.authorityFile)
        if content != expect_key:
            return False
        else:
            return True
        
    def clear(self):
        if self.fileUtil.exists(self.authorityFile):
            self.fileUtil.remove(self.authorityFile)

    @staticmethod
    def get_host_ip():
        return socket.gethostbyname(socket.gethostname())
    
