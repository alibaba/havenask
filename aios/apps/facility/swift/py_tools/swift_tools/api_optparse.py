from optparse import OptionParser

class ApiOptionParser(OptionParser):
    def exit(self, status=0, msg=None):
        errMsg = 'exit: status[%s], error[%s]' % (status, msg)
        raise ValueError(errMsg)

    def error(self, msg):
        errMsg = "%s: error[%s]" % (self.get_prog_name(), msg)
        raise ValueError(errMsg)
