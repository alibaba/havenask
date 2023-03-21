import sys
import os
import time
import threading
import json
from argparse import ArgumentParser
import traceback
hape_path = os.path.abspath(os.path.join(os.path.abspath(__file__), "../../../../../"))
sys.path.append(hape_path)

from hape.utils.logger import Logger
from hape.utils.shell import Shell
from hape.domains.target import *
from hape.utils.dict_file_util import DictFileUtil
from hape.common.constants.hape_common import HapeCommon
from hape.utils.shell import Shell
from hape.domains.admin.events.event_processor import EventProcessor

'''
domain daemon in hape admin, 
usage:
1. prcess target by event processor
2. distribute target to worker
2. collect hearbeat from worker
'''
class DomainDaemon():
    def __init__(self, global_conf, domain_name):
        self._global_conf = global_conf
        logsdir = os.path.join(self._global_conf["hape-admin"]["workdir"], HapeCommon.ADMIN_DOMAIN_DIR, "logs", domain_name)
        if not os.path.exists(logsdir):
            shell=Shell()
            shell.makedir(logsdir)
        os.chdir(logsdir)
        print("load admin daemon logging conf {}".format(self._global_conf["hape-admin"]["logging-conf"]))
        Logger.init(self._global_conf["hape-admin"]["logging-conf"])
        Logger.logger_name = "admin"
        
        self._domain_name = domain_name
        Logger.info("init admin daemon, domain name:[{}]".format(domain_name))
        self._loop_interval = float(global_conf["hape-admin"]["daemon-process-interval-sec"])
        self._domain_heartbeat_service = DomainHeartbeatService(global_conf)
        self.threads = []
        self._event_detector = EventProcessor(global_conf)
        
      
        
    def collect_heartbeat(self):
        roles = HapeCommon.ROLES
        threads = []
        for role in roles:
            workername_list  = self._domain_heartbeat_service.get_workerlist(self._domain_name, role)
            if len(workername_list) == 0:
                Logger.warning("no workers for heartbeat")
                continue
            Logger.info("get wokers list {}".format(workername_list))
            for worker_name in workername_list:
                t = threading.Thread(target=self._collect_worker_heartbeat, args=(self._domain_name, role, worker_name))
                t.name = worker_name + "-heartbeat"
                t.start()
                threads.append(t)
        
        for thread in threads:
            thread.join()   
                
        
    def _collect_worker_heartbeat(self, domain_name, role_name, worker_name):
        try:
            final_target = self._domain_heartbeat_service.read_worker_final_target(domain_name, role_name, worker_name)
            if final_target != None:
                self._domain_heartbeat_service.collect_heartbeat(domain_name, role_name, worker_name)
        except:
            Logger.warning("collect heartbeat for worker {} failed".format(worker_name))
            Logger.warning(traceback.format_exc())
            
    
    def process_target(self):
        roles = HapeCommon.ROLES
        threads = []
        for role in roles:
            workername_list  = self._domain_heartbeat_service.get_workerlist(self._domain_name, role)
            if len(workername_list) == 0:
                Logger.warning("no workers for target")
                continue
            Logger.info("get wokers list {}".format(workername_list))
            for worker_name in workername_list:
                t = threading.Thread(target=self._process_worker_target, args=(self._domain_name, role, worker_name))
                t.name = worker_name + "-target"
                Logger.info("process worker target {}".format(worker_name))
                Logger.info("current final target {}".format(self._domain_heartbeat_service.read_worker_final_target(self._domain_name, role, worker_name)))
                Logger.info("current user target {}".format(self._domain_heartbeat_service.read_worker_user_target(self._domain_name, role, worker_name)))
                threads.append(t)
                t.start()
                
        for t in threads:
            t.join()
            Logger.info("process {} target over".format(t.name))
            
    def _process_worker_target(self, domain_name, role_name, worker_name):
        try:
            self._event_detector.process(domain_name, role_name, worker_name)
        except:
            Logger.error("process target for worker {} failed".format(worker_name))
            Logger.error(traceback.format_exc())
    
    def loop_heartbeat(self):
        while True:
            try:
                Logger.info("heartbeat loop")
                self.collect_heartbeat()
            except:
                Logger.info(traceback.format_exc())
            time.sleep(self._loop_interval)
            
    def loop_target(self):
        while True:
            Logger.info("target loop")
            try:
                self.process_target()
            except:
                Logger.info(traceback.format_exc())
            time.sleep(self._loop_interval)
    
    def daemon(self):
        t = threading.Thread(target=self.loop_target ,args=())
        t.start()
        self.threads.append(t)
        t = threading.Thread(target=self.loop_heartbeat ,args=())
        t.start()
        self.threads.append(t)
                    
    
if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--global_conf")
    parser.add_argument("--domain_name")
    args = parser.parse_args()
    
    daemon = DomainDaemon(DictFileUtil.read(args.global_conf), args.domain_name)
    daemon.daemon()
    
    
    