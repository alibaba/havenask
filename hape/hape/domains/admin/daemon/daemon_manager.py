import os

from hape.utils.shell import Shell
from hape.utils.logger import Logger
from hape.utils.dict_file_util import DictFileUtil

class AdminDaemonManager():
    def __init__(self, hape_config_path):
        self._global_conf_path = os.path.join(hape_config_path, "global.conf")
        self._global_conf = DictFileUtil.read(self._global_conf_path)
        self._workdir = self._global_conf["hape-admin"]["workdir"]
        
    def domain_daemon_cmd(self, domain_name):
        daemon_cmd_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "domain_daemon.py")
        return "python {} --global_conf {} --domain_name {}".format(daemon_cmd_path, self._global_conf_path, domain_name)
        
    def start_daemon(self, domain_name):
        cmd = self.domain_daemon_cmd(domain_name)
        Logger.info("start daemon [{}]".format(cmd))
        shell = Shell()
        shell.execute_command("{}".format(cmd), is_daemon=True)
    
    def keep_daemon(self, domain_name):
        pids = self.find_daemon(domain_name)
        if self._global_conf["hape-admin"]["enable-daemon-debug"] == "True":
            Logger.info("hape admin debug mode is open, force to restart domain admin daemon")
            shell = Shell()
            shell.kill_pids(pids)
            self.start_daemon(domain_name)
        else:
            if len(pids) == 0:
                self.start_daemon(domain_name)
            elif len(pids) == 1:
                pass
            else:
               raise RuntimeError("keep one admin daemon for one domain {} failed".format(domain_name)) 
        if len(self.find_daemon(domain_name)) != 1:
            raise RuntimeError("keep one admin daemon for one domain {} failed".format(domain_name))
        
    def kill_daemon(self, domain_name):
        shell = Shell()
        pids = self.find_daemon(domain_name)
        shell.kill_pids(pids)
        pids = self.find_daemon(domain_name)
        if len(pids) !=0:
            raise RuntimeError("kill daemon for domain [{}] failed, please clean it yourself".format(domain_name))
            
        
    def find_daemon(self, domain_name):
        shell = Shell()
        response = shell.execute_command("ps -auxwww | grep -E 'domain_daemon.*domain_name {}$' | grep -v grep | awk '{{print $2}}'".format(domain_name))
        pids = []
        for raw_line in response.splitlines():
            line = raw_line.strip()
            if line.isdigit():
                pid = int(line)
                if pid == os.getppid():
                    continue
                pids.append(pid)
        return pids
                
                
            

        
            
        
        
    

    
        