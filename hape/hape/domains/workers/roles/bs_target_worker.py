import os
import sys
hape_path = os.path.abspath(os.path.join(os.path.abspath(__file__), "../../../../"))
sys.path.append(hape_path)
from hape.commands import PlanCommand

from hape.utils.logger import Logger
from hape.utils.shell import Shell

from .target_worker_base import TargetWorkerBase
import time
import json

class BsTargetWorker(TargetWorkerBase):
    def solve_starter(self, processor_target, biz_target):
        generation_id = biz_target["generation_id"]
        biz_heartbeat = self._heartbeat[self._biz_plan_key]
        Logger.info("solve worker starter")
        Logger.info("processor_target {} biz_target {} biz_heartbeat {}", processor_target, biz_target, biz_heartbeat)
        workdir = processor_target["workdir"]
        index_name = biz_target["index_name"]
        shell=Shell()
        shell.execute_command("rm -rf runtimedata")
        starter_command = "{workdir}/package/ha3_install/usr/local/bin/bs startjob -g {generation_id} -c {config_path} -n {index_name} -j local -m full -d {data_path} -w {workdir} -i ./runtimedata --pf {part_id} --pc 1 -p {partition_count} --documentformat=ha3".format(
                workdir=workdir, generation_id=generation_id, index_name = index_name, 
                config_path=biz_heartbeat["local_config_path"], data_path=biz_heartbeat["local_data_path"], partition_count=biz_target["partition_count"], part_id=biz_target["partition_id"])
        
        if "realtime_address" in biz_target:
            realtime_address, realtime_topic = biz_target["realtime_address"], biz_target["realtime_topic"]
            realtime_info = {
                "realtime_mode":"realtime_service_rawdoc_rt_build_mode",
                "data_table" : biz_target["index_name"],
                "type":"plugin",
                "module_name":"kafka",
                "module_path":"libbs_raw_doc_reader_plugin.so",
                "topic_name":realtime_topic,
                "bootstrap.servers": realtime_address,
                "src_signature":str(int(time.time())),
                "realtime_process_rawdoc": "true"
            }
            biz_heartbeat["realtime_option"] = realtime_info

            realtime_option = ' --realtimeInfo="' + json.dumps(realtime_info).replace('"','\\"')+'"'
            starter_command += realtime_option
        
        Logger.info("worker starter command {}".format(starter_command))
        out = shell.execute_command(starter_command)
        if out.find("failed")!=-1:
            self._heartbeat["runtime-message"] = out
            self._heartbeat["status"] = "failed"
        biz_target["starter"] = starter_command
        biz_target["generation_id"] = generation_id
        self._heartbeat["generation_id"] = generation_id
        self.upload_index(generation_id, biz_target)
        
    def upload_index(self, generation_id, biz_target):
        index_name = biz_target["index_name"]
        remote_index_path = biz_target["output_index_path"]
        index_path = "runtimedata"
        
        local_index_path = os.path.join(index_path, index_name, "generation_{}".format(generation_id))
        if not self._dfs_client.check(remote_index_path, True):
            self._dfs_client.makedir(remote_index_path, True)
        Logger.info("upload index from {} to {}".format(local_index_path, remote_index_path))
        upload_path = self._dfs_client.put(local_index_path, remote_index_path)
        self._heartbeat["status"] = "finished"
        self._heartbeat[self._biz_plan_key]["index_path"] = upload_path
        self._heartbeat[self._biz_plan_key]["index_name"] = index_name
        self._heartbeat[self._biz_plan_key]["partition_count"] = biz_target["partition_count"]
        self.write_heartbeat()
        
        
    def solve_data_target(self, processor_target, biz_target):
        Logger.info("solve data target")
        biz_heartbeat = self._heartbeat[self._biz_plan_key]
        index_name = biz_target["index_name"]
        Logger.info("solve index {}".format(index_name))
        data_path = biz_target["data_path"]
        work_dir = processor_target["workdir"]
        if "data_path" not in biz_heartbeat or data_path != biz_heartbeat["data_path"]:
            shell = Shell()
            shell.execute_command("rm -rf data")
            shell.makedir("data")
            local_data_path = self._dfs_client.get(data_path, work_dir)
            biz_heartbeat["local_data_path"] = local_data_path
            biz_heartbeat["data_path"] = data_path
            Logger.info("update data {}".format(local_data_path))
            Logger.info("solve data hb {}".format(self._heartbeat))
            self.write_heartbeat()
            
        Logger.info("solve index {} end".format(index_name))
                
    def watch_target(self, user_cmd, processor_target, biz_target):
        if user_cmd == PlanCommand.START_WORKERS_COMMAND:
            self.solve_processor_info(processor_target)
            self.solve_config_target(processor_target, biz_target)
            self.solve_data_target(processor_target, biz_target)
            self.solve_starter(processor_target, biz_target)
            self._hb_manager.remove_worker_final_target(self._domain_name, self._role_name, self._worker_name)

    