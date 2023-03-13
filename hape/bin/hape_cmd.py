# -*- coding: utf-8 -*-

import os
import sys
hape_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(hape_path)
sys.path.append(os.path.join(hape_path, "bin"))
from hape.commands import PlanCommand, InitCommand
from hape.utils.logger import Logger
import fire
import json

class HapeCmd(object):
    
    def _strip_domain_name(self, config_path):
        if not config_path.endswith("_hape_config"):
            raise ValueError
        else:
            name = os.path.basename(config_path).split("_")[0]
            Logger.info("parse domain nameas {}".format(name))
            return name
    
    def _check_role(self, role):
        choices = set(["all", "bs", "qrs", "searcher"])
        if role not in choices:
            log = "--role argument must be one of {}".format(choices)
            Logger.error(log)
            raise ValueError(log)
    
    def start(self, config, role, worker=None):
        """Usage: start cluster role

        Arguments:
        name: cluster name
        config: hape config
        role: role in cluster
        """
        
        name = self._strip_domain_name(config)
        self._check_role(role)
        cmd = PlanCommand(config)
    
        if role == "all":
            cmd.start(name, ["bs", "searcher", "qrs"], worker)
        else:
            cmd.start(name, [role], worker)
            
    def build_full_index(self, config):
        """Usage: build full index

        Arguments:
        name: cluster name
        config: hape config
        role: role in cluster
        """
        
        name = self._strip_domain_name(config)
        cmd = PlanCommand(config)
        cmd.start(name, ["bs"],  None)
        
            
    def remove(self, config, role):
        """Usage: remove cluster role

        Arguments:
        name: cluster name
        config: hape config
        role: role in cluster
        """
        self._check_role(role)
        name = self._strip_domain_name(config)
        cmd = PlanCommand(config)
        if role == "all":
            cmd.remove(name, ["bs", "searcher", "qrs"])
        else:
            cmd.remove(name, [role])
            
    def gs(self, config, role, final_target=False):
        """Usage: get status of cluster role

        Arguments:
        name: cluster name
        config: hape config
        role: role in cluster
        """
        self._check_role(role)
        name = self._strip_domain_name(config)
        cmd = PlanCommand(config)
        
        if role == "all":
            status = cmd.get_status(name, ["bs", "qrs", "searcher"], final_target)
        else:
            status = cmd.get_status(name, [role], final_target)
        print(json.dumps(status, indent=4))
        
            
        
        
if __name__ == "__main__":
    fire.Fire(HapeCmd)
    