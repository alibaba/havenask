import os
import sys
import traceback

class LocalFileUtil(object):
    def read(self, fileName):
        strs = ""
        list = self.__doRead(fileName)
        for str in list:
            strs = strs + str
        return strs

    def readLines(self, fileName):
        return self.__doRead(fileName)

    def cat(self, fileName):
        if not os.path.exists(fileName):
            return ""
        return self.read(fileName)

    def write(self, strs, file_name, mode='a'):
        #note that users must specify "\n" in strs for a new line 
        #                     since it is 'append' mode
        rst = False
        f = None
        try:
            try:
                f = open(file_name, mode)
                f.write(strs)
                rst = True
            except IOError, (errno, strerror):
                print "I/O error(%s): %s" % (errno, strerror)
            except ValueError:
                print "Could not convert data to an integer."
            except:
                print "Unexpected error: in FileUtils::write", sys.exc_info()[0]
                print traceback.print_exc()
        finally:
            try:
                if f:
                    f.close()
            except:
                print traceback.print_exc()
        return rst

    def __doRead(self, fileName):
        list = []
        try:
            try:
                f = open(fileName, 'r')
                list = f.readlines()
            except IOError, (errno, strerror):
                print "I/O error(%s): %s" % (errno, strerror)
            except ValueError:
                print "Could not convert data to an integer."
            except:
                print "Unexpected error: in FileUtils::__doRead()", sys.exc_info()[0]
                print traceback.print_exc()
        finally:
            try:
                f.close()
            except:
                print traceback.print_exc()
        return list
