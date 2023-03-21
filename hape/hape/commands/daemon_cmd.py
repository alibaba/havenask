# -*- coding: utf-8 -*-

import argparse

from hape.commands.cmd_base import CommandBase
from hape.domains.target import *
from hape.domains.admin.daemon.daemon_manager import AdminDaemonManager
from hape.domains.admin.events.handlers import *
from hape.utils.logger import Logger


'''
subcommand in this file will help to manage domain daemon
'''


class DaemonCommand(CommandBase, object):
    
    def kill_daemon(self, args):
        Logger.init_stdout_logger(False)
        parser = argparse.ArgumentParser()
        required_group = parser.add_argument_group('required arguments')
        required_group.add_argument('--config', type=str, dest='config', required=True, help="hape config path of havenask domain")
        self.args = parser.parse_args(args)
        self.config = self.args.config
        self._kill_daemon(self.config)
        Logger.info("kill daemon succeed")
        
    def _kill_daemon(self, config):
        daemon_manager = AdminDaemonManager(config)
        name = self._strip_domain_name(config)
        daemon_manager.kill_daemon(name)      
    
    def restart_daemon(self, args):
        Logger.init_stdout_logger(False)
        parser = argparse.ArgumentParser()
        required_group = parser.add_argument_group('required arguments')
        required_group.add_argument('--config', type=str, dest='config', required=True, help="hape config path of havenask domain")
        self.args = parser.parse_args(args)
        self.config = self.args.config
        self._restart_daemon(self.config)
        Logger.info("restart daemon succeed")
    
    def _restart_daemon(self, config):
        daemon_manager = AdminDaemonManager(config)
        name = self._strip_domain_name(config)
        daemon_manager.kill_daemon(name)
        daemon_manager.start_daemon(name)
        if len(daemon_manager.find_daemon(name)) !=1:
            raise RuntimeError("start daemon for domain {} failed".format(name))