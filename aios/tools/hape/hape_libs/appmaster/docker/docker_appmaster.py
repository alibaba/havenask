import os
from collections import OrderedDict

from .docker_util import *
from hape_libs.clusters import *
from hape_libs.common import HapeCommon
from hape_libs.appmaster import *
from hape_libs.config import DomainConfig
from hape_libs.utils.logger import Logger

class AppMasterOnDocker(HippoAppMasterBase):
    def __init__(self, key, domain_config): #type: (str, DomainConfig) -> None
        super(AppMasterOnDocker, self).__init__(key, domain_config)
        
    def get_appmaster_containername(self):
        return self._global_config.get_appmaster_service_name(self._key) + "_appmaster"

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
        
        for ip in self._global_config.get_admin_candidate_ips(self._key):
            name = self.get_appmaster_containername()
            user = self._global_config.default_variables.user,
            workdir = os.path.join(self._global_config.default_variables.user_home, name)
            
            succ = DockerContainerUtil.create_container(
                ip = ip, name = name,
                workdir = workdir, 
                homedir = self._global_config.default_variables.user_home,
                user = user,
                cpu = self._global_config.get_appmaster_base_config(self._key, "adminCpu"),
                mem = self._global_config.get_appmaster_base_config(self._key, "adminMem"),
                image = self._global_config.get_appmaster_base_config(self._key, "image"),
            )
            if not succ:
                return False
            
            succ = DockerContainerUtil.start_process(ip=ip, name=name, workdir=workdir, user=self._global_config.default_variables.user ,envs=envs, args=args, command=command)
            if not succ:
                return False
        
        return True


    def stop(self, is_delete = False, only_admin = False):
        Logger.info("Starts to stop appmaster")
        if not self._stop_appmasters():
            return False
        
        if not only_admin:
            Logger.info("Starts to stop worker")
            if not self._stop_workers():
                return False
        
        if is_delete:
            fs_wrapper = FsWrapper(self._global_config.get_service_zk_address(self._key))
            if fs_wrapper.exists(""):
                fs_wrapper.rm("")

    def _stop_appmasters(self):
        for ip in self._global_config.get_admin_candidate_ips(self._key):
            name = self.get_appmaster_containername()
            DockerContainerUtil.stop_container(ip, name)
            
        return True


    def _stop_workers(self):
        try:
            hippo_zk_address = self._global_config.get_service_hippo_zk_address(self._key)
            container_infos = self.get_assigned_hippo_worker_infos(hippo_zk_address)
            for role, container_info_list in container_infos.items():
                for container_info in container_info_list:
                    DockerContainerUtil.stop_container(container_info.ip, container_info.containerName)
        except:
            Logger.warning("Cannot parse assigned info for workers, skip stopping workers")
            
        return True

    def get_containers_status(self): #type:() -> dict[HippoAppContainerInfo]
        try:
            ## slave
            workers_container_infos = self.get_assigned_hippo_worker_infos(self._global_config.get_service_hippo_zk_address(self._key))
            admin_ip_list = self._global_config.get_admin_candidate_ips(self._key)
            
            ## master
            workers_container_infos["admin"] = []
            for ip in admin_ip_list:
                container_info = HippoAppContainerInfo(ip = ip, role = "admin", containerName = self.get_appmaster_containername())
                workers_container_infos["admin"].append(container_info)
                
            for key, container_infos in workers_container_infos.items():
                for container_info in container_infos:
                    exists = DockerContainerUtil.check_container(container_info.ip, container_info.containerName)
                    container_info.containerStatus = HippoContainerStatusType.RUNNING if exists else HippoContainerStatusType.NOT_RUNING
            return workers_container_infos
        except Exception as e:
            Logger.info("Get {} containers hippo app status failed, maybe admin already deleted".format(self._key))
            return {}
        
        
    def get_assigned_hippo_worker_infos(self, hippo_zk_address): #type: (str) -> list[HippoAppContainerInfo]
        fs_wrapper = FsWrapper(hippo_zk_address)
        info = fs_wrapper.get("assigned_slots_info")
        assigned_info_map = json.loads(info)
        container_infos = {}
        for key, values in assigned_info_map.items():
            container_infos[key] = []
            for value in values:
                valItems = value.split("&")
                ip = valItems[0]
                slot_idx = valItems[1]
                container_info = HippoAppContainerInfo(ip = ip, role = key , containerName = self._global_config.get_docker_container_prefix(self._key) + "_" + slot_idx)
                container_infos[key].append(container_info)
        return container_infos