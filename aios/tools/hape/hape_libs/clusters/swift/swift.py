import os
import json
import requests

from .swift_util import SwiftAdminService, SwiftToolWrapper
from hape_libs.appmaster import *
from hape_libs.config import *
from hape_libs.utils.logger import Logger
from hape_libs.clusters.cluster_base import *
from hape_libs.utils.fs_wrapper import FsWrapper
from hape_libs.utils.template import TemporaryDirectory

class SwiftCluster(ClusterBase):
    def __init__(self, key, domain_config):  #type: (str, DomainConfig)->None
        super(SwiftCluster, self).__init__(key, domain_config)
        self.swift_tool_wrapper = SwiftToolWrapper(self._global_config.common.binaryPath, self._master.zk_addr)
        self._proc_zfs_wrapper = FsWrapper(self._master.zk_addr)
    
    def before_master_start(self, hippo_plan):
        hadoop_home = self._global_config.common.hadoopHome
        binary_path = self._global_config.common.binaryPath
        extend_attrs = {
            "hadoop_home": hadoop_home,
            "binary_path": binary_path
        }
        fs_wrapper = FsWrapper(self._global_config.swift.swiftDataStoreRoot, extend_attrs=extend_attrs)
        if not fs_wrapper.exists(""):
            fs_wrapper.mkdir("")
        
        ## deploy config
        config_version = None
        with TemporaryDirectory() as temp_dir:
            files = ["swift/config/swift.conf", "swift/config/swift_hippo.json"]
            for file in files:
                file_path = os.path.join(temp_dir, file)
                dir_path = os.path.dirname(file_path)
                if not os.path.exists(dir_path):
                    os.makedirs(dir_path)
                with open(file_path, "w") as f:
                    f.write(self._domain_config.template_config.rel_path_to_content[file])
            config_version, succ = self.swift_tool_wrapper.deploy_config(os.path.join(temp_dir, "swift/config"))
            if succ == False:
                return None
            
        if config_version == None:
            return None
        
        ## dump version info
        service_zk_path = self._domain_config.global_config.get_service_appmaster_zk_address(self._key)
        config_zk_path = "/".join([service_zk_path, "config"])
        config_version_zk_path = "/".join([config_zk_path, config_version])
        version_info = {
            "rollback": False,
            "admin_current": config_version_zk_path,
            "broker_target_role_version": "0",
            "broker_current":  config_version_zk_path,
            "broker_target":  config_version_zk_path,
            "broker_current_role_version": "0"
        }
        
        version_zk_path = "config/version"
        if self._proc_zfs_wrapper.exists(version_zk_path):
            self._proc_zfs_wrapper.rm(version_zk_path)

        self._proc_zfs_wrapper.write(json.dumps(version_info), version_zk_path)
        
        
        ## modify hippo plan
        if config_version_zk_path == None:
            return False
        
        extra_args = [
            {
                "key": "-c",
                "value": config_version_zk_path
            }
        ]
        for arg in extra_args:
            hippo_plan.processLaunchContext.processes[0].args.append(KeyValuePair(**arg))
        
        return True
    
    def stop(self, is_delete=False):
        super(SwiftCluster, self).stop(is_delete=is_delete)
        if is_delete:
            hadoop_home = self._global_config.common.hadoopHome
            binary_path = self._global_config.common.binaryPath
            extend_attrs = {
                "hadoop_home": hadoop_home,
                "binary_path": binary_path
            }
            FsWrapper(self._global_config.swift.swiftDataStoreRoot, extend_attrs=extend_attrs).rm("")
        
    def count_alive_brokers(self):
        broker_count = 0
        try:
            http_address = self.get_leader_http_address()
            swift_admin_service = SwiftAdminService(http_address)
            processor_status_map = swift_admin_service.get_all_processor_status()
            if "admin" not in processor_status_map:
                return False
            for key, processor_status_list in processor_status_map.items():
                if key != "admin":
                    broker_count += len(processor_status_list)
        except:
            Logger.error("Failed to count alive brokers")
        return broker_count
    
    ## todo: check broker alive
    def is_ready(self):
        try:
            http_address = self.get_leader_http_address()
            swift_admin_service = SwiftAdminService(http_address)
            processor_status_map = swift_admin_service.get_all_processor_status()
            if "admin" not in processor_status_map:
                return False
            
            alive_brokers = self.count_alive_brokers()
            if alive_brokers != self._global_config.swift.brokerCount:
                Logger.warning("Broker count is not equals to {}, now: {}".format(self._global_config.swift.brokerCount, alive_brokers))
                return False
            return True
            
        except:
            Logger.warning("Failed to check is swift admin ready")
        return False
    
    def get_status(self):
        if not self.is_ready():
            Logger.error("Swift admin not ready")
            return None
        http_address = self.get_leader_http_address()
        swift_admin_service = SwiftAdminService(http_address)
        workers_status_map = self._master.get_containers_status()
        processor_status_list = swift_admin_service.get_all_processor_status()
        status_list = []
        
        ## driven by workers_status_map because processor maybe not running
        for key, worker_status_list in workers_status_map.items():
            for worker_status in worker_status_list:
                is_find = False
                for processor_status in processor_status_list[key]:
                    if processor_status.ip == processor_status.ip and processor_status.role == worker_status.role:
                        processor_status.merge_from_hippo_worker_info(worker_status)
                        if processor_status.role == "admin":
                            processor_status.processorName  = self._global_config.get_master_command(self._key)
                        else:
                            processor_status.processorName  = self._global_config.get_worker_command(self._key)
                        status_list.append(processor_status)
                        is_find = True
                        break
                if not is_find:
                    status_list.append(ClusterProcessorStatus.from_hippo_worker_info(worker_status))
            
        cluster_status = ClusterStatus(
            serviceZk = self._global_config.get_service_appmaster_zk_address(self._key),
            hippoZk = self._global_config.get_service_hippo_zk_address(self._key),
            processors = status_list
        )
        return attr.asdict(cluster_status)
    
    def get_leader_ip_address(self):
        Logger.debug("Try to parse swift admin leader from zk")
        data = self._master.admin_zk_fs_wrapper.get("admin/leader_info.json")
        leader_info = json.loads(data.strip())
        return leader_info["httpAddress"]
    
    