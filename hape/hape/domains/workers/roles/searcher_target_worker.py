import os
import sys
import json
hape_path = os.path.abspath(os.path.join(os.path.abspath(__file__), "../../../../../"))
sys.path.append(hape_path)
import socket
import httplib
from hape.commands import PlanCommand

from hape.utils.logger import Logger
from hape.utils.shell import Shell

from .target_worker_base import TargetWorkerBase

from hape.utils.logger import Logger
from hape.utils.shell import Shell
import traceback
from hape.utils.partition import get_partition_intervals
import socket
class SearcherTargetWorker(TargetWorkerBase, object):
    
    def solve_updater(self, processor_target, biz_target):
        Logger.info("solve updater")
        workdir = processor_target["workdir"]
        processor_command = "python {workdir}/package/hape/hape/domains/workers/starter/part_search_update.py --role searcher -i ./runtimedata/ -c {config_path} -p 1,2".format(
            workdir = workdir, 
            config_path=self._heartbeat[self._biz_plan_key]["local_config_path"])
        Logger.info("updater command {}".format(processor_command))
        shell=Shell()
        shell.execute_command(processor_command)
        self._heartbeat["starter"] = processor_command
        Logger.info("solve updater succeed")
        self._heartbeat["status"] = "running"
    
    def _choose_free_ports(self):
        sock = socket.socket()
        sock.bind(('', 0))
        return sock.getsockname()[1]
    
    def solve_starter(self, processor_target, biz_target):
        Logger.info("solve starter")
        workdir = processor_target["workdir"]
        shell=Shell()
        port1, port2  = self._choose_free_ports(), self._choose_free_ports()
        processor_command = "python {workdir}/package/hape/hape/domains/workers/starter/part_search_starter.py --role searcher --part_id {part_id} --part_count {part_count} -i ./runtimedata/ -c {config_path} -p {ports} -b {workdir}/package/ha3_install".format(
            workdir = workdir, 
            part_count = biz_target["partition_count"], part_id = biz_target["partition_id"], 
            config_path=self._heartbeat[self._biz_plan_key]["local_config_path"], ports = str(port1) + "," + str(port2))
        Logger.info("starter command {}".format(processor_command))
        shell=Shell()
        shell.execute_command(processor_command)
        self._heartbeat["starter"] = processor_command
        Logger.info("solve succeed")
    
    def solve_index_target(self, processor_target, biz_target):
        Logger.info("solve index target")
        
        biz_heartbeat = self._heartbeat[self._biz_plan_key]
        if "index_info" not in biz_heartbeat:
            biz_heartbeat["index_info"] = {}
            
        shell = Shell()
        Logger.info("biz target {}".format(biz_target))
        for index_name in biz_target["index_info"]:
            Logger.info("solve index {}".format(index_name))
            generation_index_path = biz_target["index_info"][index_name]["index_path"]
            generation_name = biz_target["index_info"][index_name]["index_path"].split("/")[-1]
            part_interval_list = get_partition_intervals(biz_target["index_info"][index_name]["partition_count"])
            if len(part_interval_list) == 1:
                part_interval = part_interval_list[0]
            else:
                part_interval = part_interval_list[biz_target["partition_id"]]
            index_path = os.path.join(generation_index_path, "partition_"+ str(part_interval[0]) + "_" + str(part_interval[1]))
            if index_name not in biz_heartbeat["index_info"] or biz_heartbeat["index_info"][index_name]["index_path"] != index_path:
                shell.execute_command("rm -rf runtimedata/{}".format(index_name))
                Logger.info("update index name:{} path: {}".format(index_name, index_path))
                local_index_path = self._dfs_client.get(index_path, os.path.join(processor_target["workdir"], "runtimedata", index_name, generation_name))
                
                if self._dfs_client.check(os.path.join(generation_index_path, "realtime_info.json"), False):
                    self._dfs_client.get(os.path.join(generation_index_path, "realtime_info.json"), os.path.join(processor_target["workdir"], "runtimedata", index_name, generation_name))
                if self._dfs_client.check(os.path.join(generation_index_path, "partition_count"), False):
                    self._dfs_client.get(os.path.join(generation_index_path, "partition_count"), os.path.join(processor_target["workdir"], "runtimedata", index_name, generation_name))

                biz_heartbeat["index_info"][index_name] = {}
                biz_heartbeat["index_info"][index_name]["index_path"] = index_path
                biz_heartbeat["index_info"][index_name]["local_index_path"] = local_index_path
                self.solve_starter(processor_target, biz_target)
                self.write_heartbeat()
            Logger.info("solve index {} succeed".format(index_name))
        Logger.info("solve index target succeed")
    
    
    def curl(self, address, method, request, curl_type='POST'):
        try:
            conn = httplib.HTTPConnection(address, timeout=3)
            conn.request(curl_type, method, json.dumps(request))
            r = conn.getresponse()
            data = r.read()
            retCode = 0
            if r.status != 200:
                retCode = -1
            return retCode, data, r.reason, r.status
        except Exception as e:
            return -1, '', str(e), 418
    
    def solve_ready_zones(self, processor_target, biz_target):
        Logger.error("solve ready_zones")
        try:
            shell=Shell()
            pid = shell.get_pids("sap_server_d")[0]
            ports = shell.get_listen_port(pid)
            Logger.info("check heartbeat pid:{} http_port:{}".format(pid, ports[1]))
            ret_code, out, err, _ = self.curl("localhost:{}".format(ports[1]), "/HeartbeatService/heartbeat", {}, curl_type="GET")
            # response, code = self.curl("curl localhost:{}/HeartbeatService/heartbeat".format(ports[1]))
            heartbeat = json.loads(out)
            
            serviceInfo = json.loads(heartbeat["serviceInfo"])
            ready_zone = {  }
            infos = serviceInfo["cm2"]["topo_info"].strip('|').split('|')
            for info in infos:
                splitInfo = info.split(':')
                localConfig = {}
                localConfig["biz_name"] = splitInfo[0]
                localConfig["part_count"] = int(splitInfo[1])
                localConfig["part_id"] = int(splitInfo[2])
                localConfig["version"] =  int(splitInfo[3])
                localConfig["ip"] = socket.gethostbyname(socket.gethostname())
                localConfig["tcp_port"] = int(ports[0])
                zoneName = splitInfo[0] + "_" + str(biz_target["partition_id"])
                ready_zone[zoneName] = localConfig
                Logger.info("gen localConfig: {}".format(localConfig))
            self._heartbeat[self._biz_plan_key]["ready_zone"] = ready_zone
            Logger.info("gen ready_zone: {}".format(ready_zone))
            self._heartbeat["status"] = "running"
            self.write_heartbeat()
        except:
            Logger.error("solve ready_zones failed")
            Logger.error(traceback.format_exc())
            return False
        Logger.error("solve ready_zones succeed")
        return True
    
    
            
    def watch_target(self, user_cmd, processor_target, biz_target):
        if user_cmd == PlanCommand.START_WORKERS_COMMAND:
            self.solve_processor_info(processor_target)
            self.solve_config_target(processor_target, biz_target)
            self.solve_index_target(processor_target, biz_target)
            if self.solve_ready_zones(processor_target, biz_target):
                self._hb_manager.remove_worker_final_target(self._domain_name, self._role_name, self._worker_name)

    