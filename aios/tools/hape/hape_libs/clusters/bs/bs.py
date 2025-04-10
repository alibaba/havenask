import os
import json
import requests

from hape_libs.clusters.cluster_base import *
from .bs_util import BsAdminService
from hape_libs.utils.logger import Logger

class BsCluster(ClusterBase):
    def __init__(self, key, domain_config):
        super(BsCluster, self).__init__(key, domain_config)
    
    def get_leader_ip_address(self):
        Logger.debug("Try to parse bs admin leader from zk")
        try:
            data = self._master.admin_zk_fs_wrapper.get("admin/LeaderElection/leader_election0000000000")
            return json.loads(data)["httpAddress"]
        except:
            Logger.info("Bs admin is not started by now")
        return None
    
    def is_ready(self):
        address = self.get_leader_http_address()
        if address == None:
            return False
        try:
            response = requests.get("{}/__method__".format(address))
            return response.text.find("AdminService") != -1
        except:
            return False
    
    def get_admin_service(self):
        leader_http_address = self.get_leader_http_address()
        if leader_http_address != None:
            admin_service = BsAdminService(leader_http_address)
            return admin_service
        else:
            return None
        
    def get_status(self, table = None):
        if not self.is_ready():
            Logger.error("Bs admin not ready")
            return None  
        admin_service =  self.get_admin_service()      
        # processors_status_map = admin_service.get_processors_status()
        workers_status_map = self._master.get_containers_status()
        status_list = []
        for key, worker_status_list in workers_status_map.items():
            for worker_status in worker_status_list:
                processor_status = ClusterProcessorStatus.from_hippo_worker_info(worker_status)
                if key == "admin":
                    processor_status.processorName = self._global_config.get_master_command(self._key)
                    processor_status.processorStatus = ProcessorStatusType.RUNNING
                    status_list.append(processor_status)
                else:
                    if table != None and worker_status.role.find("."+table+".") == -1:
                        continue
                    processor_status  = ClusterProcessorStatus.from_hippo_worker_info(worker_status)
                    processor_status.processorName = self._global_config.get_worker_command(self._key)
                    processor_status.processorStatus = ProcessorStatusType.RUNNING
                    status_list.append(processor_status)
        cluster_status = ClusterStatus(
            leaderAddress = self.get_leader_http_address(),
            serviceZk = self._global_config.get_service_appmaster_zk_address(self._key),
            hippoZk = self._global_config.get_service_hippo_zk_address(self._key),
            processors = status_list
        )
        return attr.asdict(cluster_status)
            