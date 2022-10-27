# -*- coding: utf-8 -*-

from base_cmd import BaseCmd
from tools_config import ToolsConfig
from process import Process

class AppBaseCmd(BaseCmd):
    def __init__(self):
        super(AppBaseCmd, self).__init__()

    def addOptions(self):
        self.parser.add_option('-c', '--config', action='store', dest='configPath')

    def checkOptionsValidity(self, options):
        if not options.configPath:
            raise Exception('-c config_path is needed.')

    def initMember(self, options):
        self.configPath = options.configPath

    def hippoAdapter(self, action, stdout=False):
        latestConfig = self.getLatestConfig(self.configPath)
        process = Process()
        cmd = self.toolsConf.getHippoAdapter() + ' %s %s' % (latestConfig, action)
        out, err, code = process.run(cmd)
        if stdout:
            print out
        if code != 0:
            raise Exception('%s admin failed: data[%s] error[%s] code[%d]' %
                            (action, out, err, code))

class StartServiceCmd(AppBaseCmd):
    '''
    bs sts -c config_path

options:
    -c config_path               : required, config path for build service

examples:
    bs sps -c /path/to/config
    '''
    def __init__(self):
        super(StartServiceCmd, self).__init__()

    def run(self):
        self.hippoAdapter('start')

class StopServiceCmd(AppBaseCmd):
    '''
    bs sps -c config_path

options:
    -c config_path               : required, config path for build service

examples:
    bs sps -c /path/to/config
    '''
    def __init__(self):
        super(StopServiceCmd, self).__init__()

    def run(self):
        self.hippoAdapter('stop')

class UpdateAdminCmd(AppBaseCmd):
    '''
    bs upa -c config_path

options:
    -c config_path               : required, config path for build service

examples:
    bs upa -c /path/to/config
    '''
    def __init__(self):
        super(UpdateAdminCmd, self).__init__()

    def run(self):
        self.hippoAdapter('update')

class GetAppStatusCmd(AppBaseCmd):
    '''
    bs gas -c config_path

options:
    -c config_path               : required, config path for build service

examples:
    bs gas -c /path/to/config
    '''
    def __init__(self):
        super(GetAppStatusCmd, self).__init__()

    def run(self):
        self.hippoAdapter('status', True)
