import json
import os

from .proc_util import LocalProcUtil

from hape_libs.clusters import *
from hape_libs.appmaster import *
from hape_libs.config import DomainConfig

class AppMasterOnLocalProcessor(HippoAppMasterBase):
    def __init__(self, key, domain_config): #type: (str, DomainConfig) -> None
        super(AppMasterOnLocalProcessor, self).__init__(key, domain_config)
    
    def start(self, hippo_plan): #type:(HippoMasterPlan) -> bool
        prepare_candidate_ips(
            self._global_config.get_service_hippo_zk_address(self._key), 
            self._global_config.get_worker_candidate_maps(self._key)
        )
        
        process = hippo_plan.processLaunchContext.processes[0]
        envs = []
        for env in  process.envs:
            envs.append(env.key + "=" + str(env.value))
        
        args = []
        for arg in process.args:
            args.append(arg.key + " " + str(arg.value))
            
        command = process.cmd
        workdir = os.path.join(self._global_config.default_variables.user_home, self.service_name+"_appmaster")
        succ = LocalProcUtil.start_process(workdir, envs, args, command)
        return succ

    def stop(self, is_delete = False):
        raw_hippo_plan = self._domain_config.template_config.get_rendered_appmaster_plan(self._key)
        hippo_plan = HippoMasterPlan(**raw_hippo_plan)
        process = hippo_plan.processLaunchContext.processes[0]
        
        ## stop appmaster
        LocalProcUtil.stop_process(process.cmd)
                
        container_map = self.get_containers_status()
        ## stop workers
        for key in container_map:
            if key != "admin":
                ## temporary solve havenask role
                LocalProcUtil.stop_process(key.split(".")[0])

        if is_delete :
            fs_wrapper = FsWrapper(self._global_config.get_service_zk_address(self._key))
            if fs_wrapper.exists(""):
                fs_wrapper.rm("")
    
    def get_containers_status(self): #type:() -> dict[HippoAppContainerInfo]
        try:
            ## slave
            workers_container_infos = self.get_assigned_hippo_worker_infos(self._global_config.get_service_hippo_zk_address(self._key))
            admin_ip_list = self._global_config.get_admin_candidate_ips(self._key)
            
            ## master
            workers_container_infos["admin"] = []
            for ip in admin_ip_list:
                container_info = HippoAppContainerInfo(ip = ip, role = "admin", containerName = "PROC")
                workers_container_infos["admin"].append(container_info)
                
            for key, container_infos in workers_container_infos.items():
                for container_info in container_infos:
                    container_info.containerStatus = HippoContainerStatusType.RUNNING
            return workers_container_infos
        except Exception as e:
            Logger.info("Get {} containers hippo app status failed, maybe admin already deleted".format(self._key))
            return {}
        
        
    def get_assigned_hippo_worker_infos(self, hippo_zk_address): #type: (str) -> list[HippoAppContainerInfo]
        fs_wrapper = FsWrapper(hippo_zk_address)
        info = fs_wrapper.get("assigned_slots_info")
        assigned_info_map = json.loads(info)
        container_infos = {}
        service_name = self._global_config.get_appmaster_service_name(self._key)
        for key, values in assigned_info_map.items():
            container_infos[key] = []
            for value in values:
                valItems = value.split("&")
                ip = valItems[0]
                slot_idx = valItems[1]
                # container_name = "havenask_container" + "_" + service_name + "_" + str(slot_idx)
                container_info = HippoAppContainerInfo(ip = ip, role = key , containerName = "PROC")
                container_infos[key].append(container_info)
        return container_infos
        
        
