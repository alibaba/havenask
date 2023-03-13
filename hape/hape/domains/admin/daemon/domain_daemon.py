import sys
import os
import json
hape_path = os.path.abspath(os.path.join(os.path.abspath(__file__), "../../../../../"))
sys.path.append(hape_path)
from hape.utils.logger import Logger
from hape.utils.shell import Shell
from argparse import ArgumentParser
import traceback
from hape.domains.target import *
from hape.utils.dict_file_util import DictFileUtil
from hape.common.constants.hape_common import HapeCommon
from hape.utils.shell import Shell
from hape.domains.admin.events.event_processor import EventProcessor
import time
import threading

    
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
        Logger.info("init admin aconfn, cluster name:[{}]".format(domain_name))
        self._loop_interval = float(global_conf["hape-admin"]["target-process-interval-sec"])
        self._domain_hb_service = DomainHeartbeatService(global_conf)
        self.threads = []
        self._event_detector = EventProcessor(global_conf)
        
      
        
    def collect_hb(self):
        roles = HapeCommon.ROLES
        threads = []
        for role in roles:
            workername_list  = self._domain_hb_service.get_workerlist(self._domain_name, role)
            if len(workername_list) == 0:
                Logger.warning("no workers for hb")
                continue
            Logger.info("get wokers list {}".format(workername_list))
            for worker_name in workername_list:
                t = threading.Thread(target=self._collect_worker_hb, args=(self._domain_name, role, worker_name))
                t.name = worker_name + "-heartbeat"
                t.start()
                threads.append(t)
        
        for thread in threads:
            thread.join()   
                
        
    def _collect_worker_hb(self, domain_name, role_name, worker_name):
        try:
            final_target = self._domain_hb_service.read_worker_final_target(domain_name, role_name, worker_name)
            if final_target != None:
                self._domain_hb_service.collect_heartbeat(domain_name, role_name, worker_name)
        except:
            Logger.warning("collect heartbeat for worker {} failed".format(worker_name))
            Logger.warning(traceback.format_exc())
            
    
    def process_target(self):
        roles = HapeCommon.ROLES
        threads = []
        for role in roles:
            workername_list  = self._domain_hb_service.get_workerlist(self._domain_name, role)
            if len(workername_list) == 0:
                Logger.warning("no workers for target")
                continue
            Logger.info("get wokers list {}".format(workername_list))
            for worker_name in workername_list:
                Logger.info("process worker target {}".format(worker_name))
                Logger.info("current final target {}".format(self._domain_hb_service.read_worker_final_target(self._domain_name, role, worker_name)))
                Logger.info("current user target {}".format(self._domain_hb_service.read_worker_user_target(self._domain_name, role, worker_name)))
                t = threading.Thread(target=self._process_worker_target, args=(self._domain_name, role, worker_name))
                t.name = worker_name + "-target"
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
    
    def loop_hb(self):
        while True:
            try:
                Logger.info("heartbeat loop")
                self.collect_hb()
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
        self.threads.append(threading.Thread(target=self.loop_target ,args=()).start())
        self.threads.append(threading.Thread(target=self.loop_hb ,args=()).start())
                    
    
if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--global_conf")
    parser.add_argument("--domain_name")
    args = parser.parse_args()
    
    daemon = DomainDaemon(DictFileUtil.read(args.global_conf), args.domain_name)
    daemon.daemon()
    
    
    