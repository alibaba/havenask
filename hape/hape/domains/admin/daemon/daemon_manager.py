import os

from hape.utils.shell import Shell
from hape.utils.logger import Logger
from hape.utils.dict_file_util import DictFileUtil

class AdminDaemonManager():
    def __init__(self, hape_config_path):
        self._global_conf_path = os.path.join(hape_config_path, "global.conf")
        self._global_conf = DictFileUtil.read(self._global_conf_path)
        
    def domain_daemon_cmd(self, domain_name):
        daemon_cmd_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "domain_daemon.py")
        return "python {} --global_conf {} --domain_name {}".format(daemon_cmd_path, self._global_conf_path, domain_name)
        
    def start_daemon(self, domain_name):
        cmd = self.domain_daemon_cmd(domain_name)
        Logger.info("start daemon [{}]".format(cmd))
        shell=Shell()
        shell.execute_command("{}".format(cmd), is_daemon=True)
    
    def find_daemon(self, domain_name):
        cmd = self.domain_daemon_cmd(domain_name)
        shell=Shell()
        pids = shell.get_pids(cmd)
        Logger.info("find admin daemon pids {}".format(pids))
        return pids

    def stop_daemon(self, domain_name):
        cmd = self.domain_daemon_cmd(domain_name)
        shell=Shell()
        pids = shell.get_pids(cmd)
        if len(pids) != 0:
            shell=Shell()
            shell.kill_pids(pids)
    
    def keep_daemon(self, domain_name):
        pids = self.find_daemon(domain_name)
        if self._global_conf["hape-admin"]["debug-mode"]:
            Logger.info("hape admin debug mode is open, force to restart domain admin daemon")
            self.stop_daemon(domain_name)
            self.start_daemon(domain_name)
        else:
            sz = len(pids)
            if sz == 0:
                self.start_daemon(domain_name)
            elif sz == 1:
                pass
            else:
                self.stop_daemon(domain_name)
                self.start_daemon(domain_name)
            
        ## include parent process
        if len(self.find_daemon(domain_name)) != 1:
            raise RuntimeError("keep one admin daemon for one domain {} failed".format(domain_name))
        
            
        
        
    

    
        