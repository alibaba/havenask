import time
import os
import sys
hape_path = os.path.abspath(os.path.join(os.path.abspath(__file__), "../../../../../"))
sys.path.append(hape_path)

from hape.domains.workers.heartbeat_manager import HeartBeatManager
from hape.common.constants.hape_common import HapeCommon
from hape.common.dfs.dfs import DfsClientFactory
from hape.domains.target import Target
from hape.utils.logger import Logger
from hape.utils.shell import Shell
from hape.utils.shell import Shell

import traceback
import shutil
import threading
import json


class TargetWorkerBase:
    def __init__(self, global_conf, domain_name, role_name, worker_name):  #type:(dict, str, str, str)->None
        # print(global_conf["hape-worker"]["logging-conf"])
        self._domain_name = domain_name
        self._role_name = role_name
        self._worker_name = worker_name
        self._dfs_client = DfsClientFactory.create(global_conf)
        self._biz_plan_key = HapeCommon.ROLE_BIZ_PLAN_KEY[self._role_name]
        self._heartbeat = {"processor_info":{}, self._biz_plan_key:{}}
        self._heartbeat_manager = HeartBeatManager(self._worker_name, global_conf)
        self._threads = []
            
    
    def watch_target(self, user_cmd, processor_target, biz_target):
        raise NotImplementedError
            
    
    def solve_processor_info(self, processor_target):
        Logger.info("solve processor info")
        self._heartbeat.update({"processor_info":processor_target})
        self.solve_packages_target(processor_target)
        self.solve_env_and_args(processor_target)
        self._heartbeat['status'] = "starting"
        self.write_heartbeat()
        
    def solve_env_and_args(self, processor_target):
        Logger.info("solve env and args target")
        envs = processor_target["envs"]
        args = processor_target["args"]
        with open("heartbeats/envs_and_args.json", "w") as f:
            json.dump({"envs": envs, "args": args}, f)


    def solve_packages_target(self, processor_target):
        Logger.info("solve packages target")
        packages = processor_target["packages"]
        workdir = processor_target["workdir"]
        Logger.info("deploy packages {}".format(packages))
        for package in packages:
            self._dfs_client.get(package, os.path.join(workdir, "package"))
            Logger.info("install package {} succeed".format(package))
                
    
    def solve_config_target(self, processor_target, biz_target):
        Logger.info("solve config target")
        self._heartbeat['status'] = "starting"
        config_path = biz_target["config_path"]
        biz_heartbeat = self._heartbeat[self._biz_plan_key]
        Logger.info("biz_heartbeat {}".format(biz_heartbeat))
        work_dir = processor_target["workdir"]
        if 'config_path' not in biz_heartbeat or biz_heartbeat['config_path'] != config_path:
            shell= Shell()
            shell.execute_command("rm -rf config")
            shell.execute_command("rm -rf worker_config")
            shell.makedir("worker_config")
            Logger.info("try to get config file {}".format(config_path))
            local_config_path = self._dfs_client.get(config_path, os.path.join(work_dir, "worker_config"))
            Logger.info("update config {}".format(local_config_path))
            biz_heartbeat["config_path"] = config_path
            dest_path = os.path.join(work_dir, "worker_config", ("online_config" if self._role_name !="bs" else  "offline_config"))
            shutil.move(local_config_path, dest_path)
            biz_heartbeat["local_config_path"] = dest_path
            self.write_heartbeat()
        else:
            Logger.info("no need to update config path")
            
    def schedule_write_heartbeat(self):
        raise NotImplementedError
         
    def write_heartbeat(self):
        Logger.info("write heartbeat {}".format(self._heartbeat))
        self._heartbeat_manager.write_worker_heartbeat(
            self._domain_name, self._role_name, self._worker_name,
            Target(Target.HEARTBEAT, self._heartbeat, self._domain_name, self._role_name, self._worker_name).to_dict()
        )
        
    def loop_schedule_write_heartbeat(self):
        while True:
            try:
                Logger.info("heartbeat loop")
                self.schedule_write_heartbeat()
            except:
                Logger.warning("schedually write heartbeat failed")
                Logger.warning(traceback.format_exc())
            time.sleep(5)
        
            
    def loop_watch(self):
        while True:
            try:
                Logger.info("watch loop")
                Logger.info("current heart beat {}".format(self._heartbeat))
                final_target = self._heartbeat_manager.read_worker_final_target(self._domain_name, self._role_name, self._worker_name)
                if final_target == None:
                    Logger.info("no new targets, skip")
                    continue
                else:
                    plan = final_target["plan"]
                    user_cmd = final_target["user_cmd"]
                    Logger.info("unreached targets exists")
                    Logger.info("current target {}".format(final_target))
                    self.watch_target(user_cmd, plan[HapeCommon.PROCESSOR_INFO_KEY], plan[HapeCommon.ROLE_BIZ_PLAN_KEY[self._role_name]])
            except:
                Logger.error(traceback.format_exc())
            Logger.info("sleep")
            time.sleep(5)   
    
    def run(self):
        shell=Shell()
        pids = shell.get_pids("worker_daemon")
        shell.kill_pids(pids)
        t = threading.Thread(target=self.loop_watch, args=())
        t.name = "watch-target"
        t.start()
        self._threads.append(t)
        t = threading.Thread(target=self.loop_schedule_write_heartbeat, args=())
        t.name = "schedule-heartbeat"
        t.start()
        self._threads.append(t)
        