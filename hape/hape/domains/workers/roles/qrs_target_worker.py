import os
import sys
import json
hape_path = os.path.abspath(os.path.join(os.path.abspath(__file__), "../../../../../"))
sys.path.append(hape_path)
from hape.commands import PlanCommand

from hape.utils.logger import Logger
from hape.utils.shell import Shell

from .target_worker_base import TargetWorkerBase

from hape.utils.logger import Logger
from hape.utils.shell import Shell
from hape.utils.partition import get_partition_intervals
import traceback
import socket

class QrsTargetWorker(TargetWorkerBase ,object):
    def solve_updater(self, processor_target, biz_target):
        Logger.info("solve updater")
        workdir = processor_target["workdir"]
        processor_command = "python {workdir}/package/hape/hape/domains/workers/starter/part_search_update.py --role qrs -i ./runtimedata/ -c {config_path} -p 1,2".format(
            workdir = workdir, 
            config_path=self._heartbeat[self._biz_plan_key]["local_config_path"])
        Logger.info("updater command {}".format(processor_command))
        shell=Shell()
        shell.execute_command(processor_command)
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
        processor_command = "python {workdir}/package/hape/hape/domains/workers/starter/part_search_starter.py --role qrs -i ./runtimedata/ -c {config_path} -p {ports} -b {workdir}/package/ha3_install --qrsHttpArpcBindPort {qrs_http_port}".format(
            workdir = workdir, 
            config_path=self._heartbeat[self._biz_plan_key]["local_config_path"],  ports = str(port1) + "," + str(port2), qrs_http_port=qrs_http_port)
        # Logger.info("starter command {}".format(processor_command))
        shell=Shell()
        shell.execute_command(processor_command)
        self._heartbeat["starter"] = processor_command
        self._heartbeat[self._biz_plan_key]["qrs_http_port"] =  qrs_http_port
        Logger.info("solve succeed")
    
    def solve_subscribe(self, processor_target, biz_target):
        try:
            Logger.info("solve subscribe")
            ready_zones = biz_target["ready_zones"]
            max_version = -1
            for ready_zone in ready_zones:
                max_version = max(max_version, ready_zone["version"])
                
            Logger.info("use max_version={} as version".format(max_version))

            for ready_zone in ready_zones:
                ready_zone["version"] = max_version
            
            
            if "ready_zones" not in self._heartbeat or json.dumps(ready_zones) != json.dumps(self._heartbeat["ready_zones"]):
                Logger.info("restart qrs")
                Logger.info("ready zones {}".format(ready_zones))
                with open("heartbeats/qrs_subscribe.json", "w") as f:
                    json.dump(ready_zones, f)
                self.solve_starter(processor_target, biz_target)
                self._heartbeat[self._biz_plan_key]["ready_zones"] = ready_zones
                self._heartbeat["status"] = "running"
                Logger.info("current heartbeat {}".format(self._heartbeat))
                self.write_heartbeat()
        except:
            Logger.error("solve subcribe failed")
            Logger.error(traceback.format_exc())
            return False
        Logger.info("solve subscribe succeed")
        return True
            
    
    def watch_target(self, user_cmd, processor_target, biz_target):
        if user_cmd == PlanCommand.START_WORKERS_COMMAND:
            self.solve_processor_info(processor_target)
            self.solve_config_target(processor_target, biz_target)
            if self.solve_subscribe(processor_target, biz_target):
                self._hb_manager.remove_worker_final_target(self._domain_name, self._role_name, self._worker_name)
                