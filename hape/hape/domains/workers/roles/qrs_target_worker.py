import os
import sys
import json
hape_path = os.path.abspath(os.path.join(os.path.abspath(__file__), "../../../../../"))
sys.path.append(hape_path)

from hape.utils.logger import Logger
from hape.utils.shell import Shell
from hape.common import HapeCommon


from .target_worker_base import TargetWorkerBase

from hape.utils.logger import Logger
from hape.utils.shell import Shell
import traceback
import socket

class QrsTargetWorker(TargetWorkerBase ,object):
    def solve_updater(self, processor_target, biz_target):
        Logger.info("solve updater")
        workdir = processor_target["workdir"]
        processor_command = "python {workdir}/package/hape/domains/workers/starter/part_search_update.py --role qrs -i ./runtimedata/ -c {config_path} -p 1,2".format(
            workdir = workdir, 
            config_path=self._heartbeat[self._biz_plan_key]["local_config_path"])
        Logger.info("updater command {}".format(processor_command))
        shell=Shell()
        response = shell.execute_command(processor_command)
        if response.find("success") == -1:
            raise RuntimeError("update qrs failed")
        Logger.info("solve updater succeed")
        self._heartbeat["status"] = "running"
    
    def _choose_free_ports(self):
        sock = socket.socket()
        sock.bind(('', 0))
        return sock.getsockname()[1]
    
    def solve_starter(self, processor_target, biz_target):
        Logger.info("solve starter")
        workdir = processor_target["workdir"]
        qrs_http_port = biz_target["qrs_http_port"]
        shell=Shell()
        port1, port2  = self._choose_free_ports(), self._choose_free_ports()
        shell.kill_pids(shell.get_pids("sap_server_d"))
        processor_command = "python {workdir}/package/hape/domains/workers/starter/part_search_starter.py --role qrs -i ./runtimedata/ -c {config_path} -p {ports} -b {workdir}/package/ha3_install --qrsHttpArpcBindPort {qrs_http_port}".format(
            workdir = workdir, 
            config_path=self._heartbeat[self._biz_plan_key]["local_config_path"],  ports = str(port1) + "," + str(port2), qrs_http_port=qrs_http_port)
        self._heartbeat["starter"] = processor_command
        # Logger.info("starter command {}".format(processor_command))
        shell=Shell()
        response = shell.execute_command(processor_command)
        if response.find("success") == -1:
            raise RuntimeError("start qrs failed")
        # self._heartbeat["starter"] = processor_command
        self._heartbeat[self._biz_plan_key]["qrs_http_port"] =  qrs_http_port
        Logger.info("solve succeed")
    
    def solve_subscribe(self, processor_target, biz_target):
        try:
            Logger.info("solve subscribe")
            subscribe_infos = biz_target["subscribe_infos"]
            max_version = -1
            for subscribe_info in subscribe_infos:
                max_version = max(max_version, subscribe_info["version"])
                
            Logger.info("use max_version={} as version".format(max_version))

            for subscribe_info in subscribe_infos:
                subscribe_info["version"] = max_version
            
            
            if "subscribe_infos" not in self._heartbeat or json.dumps(subscribe_infos) != json.dumps(self._heartbeat["subscribe_infos"]):
                Logger.info("restart qrs")
                Logger.info("subscribe info {}".format(subscribe_infos))
                with open("heartbeats/qrs_subscribe.json", "w") as f:
                    json.dump(subscribe_infos, f)
                self.solve_starter(processor_target, biz_target)
                self._heartbeat[self._biz_plan_key]["subscribe_infos"] = subscribe_infos
                self._heartbeat["status"] = "running"
                Logger.info("current heartbeat {}".format(self._heartbeat))
                self.write_heartbeat()
        except:
            Logger.error("solve subcribe failed")
            Logger.error(traceback.format_exc())
            return False
        Logger.info("solve subscribe succeed")
        return True
    
    def schedule_write_heartbeat(self):
        if "status" not in self._heartbeat:
            Logger.info("status is not set, not write heartbeat")
            return
        else:
            pids = Shell().get_pids("sap_server_d")
            Logger.info("sap_server_d pids:[{}]".format(pids))
            if len(pids) == 0:
                Logger.info("sap_server_d processor not found, not write heartbeat")
                self._heartbeat["status"] = "starting"
                return 
        super(QrsTargetWorker, self).write_heartbeat()
    
    
    def check_curl(self, biz_target):
        pass
    
    def watch_target(self, user_cmd, processor_target, biz_target):
        if user_cmd == HapeCommon.START_WORKER_COMMAND or user_cmd == HapeCommon.DP_COMMAND:
            self.solve_processor_info(processor_target)
            self.solve_config_target(processor_target, biz_target)
            if self.solve_subscribe(processor_target, biz_target):
                self._heartbeat_manager.remove_worker_final_target(self._domain_name, self._role_name, self._worker_name)
                
        elif user_cmd == HapeCommon.UPC_COMMAND:
            self.solve_config_target(processor_target, biz_target)
            self.solve_updater(processor_target, biz_target)
            self._heartbeat_manager.remove_worker_final_target(self._domain_name, self._role_name, self._worker_name)

        
