import os
from sched import scheduler
import requests
import json
import attr

from .service.catalog_manager import CatalogManager
from .service.suez_cluster_manager import SuezClusterManager
from .service.scheduler_service import SchedulerService
from hape_libs.utils.logger import Logger
from hape_libs.config import *
from hape_libs.appmaster import HippoMasterPlan, KeyValuePair
from hape_libs.clusters.cluster_base import *
from hape_libs.common import HapeCommon
from hape_libs.utils.fs_wrapper import FsWrapper

class SuezCluster(ClusterBase):
    def __init__(self, key, domain_config): #type:(str, DomainConfig)->None
        super(SuezCluster, self).__init__(key, domain_config)
        self.catalog_manager = self.get_catalog_manager()
        self.suez_cluster_manager = self.get_suez_cluster_manager()

    def get_catalog_manager(self):
        leader_http_address = self.get_leader_http_address()
        if leader_http_address != None:
            catalog_manager = CatalogManager(leader_http_address, self._domain_config)
            return catalog_manager
        else:
            return None
        
    def get_scheduler_service(self):
        leader_http_address = self.get_leader_http_address()
        if leader_http_address != None:
            scheduler_service = SchedulerService(leader_http_address)
            return scheduler_service
        else:
            return None

    def get_suez_cluster_manager(self):
        leader_http_address = self.get_leader_http_address()
        if leader_http_address != None:
            self.suez_cluster_manager = SuezClusterManager(leader_http_address,
                                                        self._global_config.get_service_appmaster_zk_address(HapeCommon.SWIFT_KEY),
                                                        self._global_config.get_service_appmaster_zk_address(HapeCommon.HAVENASK_KEY),
                                                        self._domain_config)
            return self.suez_cluster_manager

    def is_ready(self):
        try:
            address = self.get_leader_http_address()
            Logger.debug("Check catalog service in address: {}".format(address))
            response = requests.post("{}/CatalogService/listCatalog".format(address), timeout=2)
            data = response.json()
            Logger.debug("Catalog service response: {}".format(str(data)))
            if len(data['status']) != 0:
                Logger.debug("Catalog service is not ready in havenask admin by now")
                return False

            Logger.debug("Check cluster service in address: {}".format(address))
            response = requests.post("{}/ClusterService/getClusterDeployment".format(address), timeout=2)
            data = response.text
            Logger.debug("Cluster service response: {}".format(str(data)))
            if data.find("SERVICE_NOT_READY") == -1:
                return True
            else:
                Logger.debug("Cluster service is not ready in havenask admin by now")
                return False

        except Exception as e:
            return False

    def before_master_start(self, hippo_plan): #type:(str, HippoMasterPlan) -> bool
        hadoop_home = self._global_config.common.hadoopHome
        binary_path = self._global_config.common.binaryPath
        extend_attrs = {
            "hadoop_home": hadoop_home,
            "binary_path": binary_path
        }
        fs_wrapper = FsWrapper(self._global_config.havenask.dbStoreRoot, extend_attrs=extend_attrs)
        if not fs_wrapper.exists(""):
            fs_wrapper.mkdir("")
        fs_wrapper = FsWrapper(self._global_config.havenask.suezClusterStoreRoot, extend_attrs=extend_attrs)
        if not fs_wrapper.exists(""):
            fs_wrapper.mkdir("")
        fs_wrapper = FsWrapper(self._global_config.common.dataStoreRoot, extend_attrs=extend_attrs)
        if not fs_wrapper.exists(""):
            fs_wrapper.mkdir("")
        
        process = hippo_plan.processLaunchContext.processes[0]
        template_addr = self._domain_config.template_config.upload_direct_table_templ()
        process.envs.append(KeyValuePair(key='DIRECT_WRITE_TEMPLATE_CONFIG_PATH', value=template_addr))
        template_addr = self._domain_config.template_config.upload_offline_table_templ()
        process.envs.append(KeyValuePair(key='BS_TEMPLATE_CONFIG_PATH', value=template_addr))
        return True

    def after_master_start(self):
        self.catalog_manager = self.get_catalog_manager()
        self.catalog_manager.create_default_catalog()
        self.suez_cluster_manager = self.get_suez_cluster_manager()
        self.suez_cluster_manager.create_or_update_default_clusters()
        
    def stop(self, is_delete=False, only_admin = False):
        super(SuezCluster, self).stop(is_delete=is_delete, only_admin = only_admin)
        hadoop_home = self._global_config.common.hadoopHome
        binary_path = self._global_config.common.binaryPath
        extend_attrs = {
            "hadoop_home": hadoop_home,
            "binary_path": binary_path
        }
        if is_delete:
            FsWrapper(self._global_config.havenask.dbStoreRoot, extend_attrs=extend_attrs).rm("")
            FsWrapper(self._global_config.havenask.suezClusterStoreRoot, extend_attrs=extend_attrs).rm("")
            FsWrapper(self._global_config.common.dataStoreRoot, extend_attrs=extend_attrs).rm("templates")
        
    def get_leader_ip_address(self):
        Logger.debug("Try to parse suez admin leader from zk")
        try:
            data = self._master.admin_zk_fs_wrapper.get("admin/LeaderElection/leader_election0000000000")
            return data.split("\n")[-1]
        except:
            Logger.info("Suez admin is not started by now")
        return None
    
    
    def wait_cluster_ready(self):
        def check_ready():
            status = self.get_status()
            return status['clusterStatus'] == "READY"
        succ = Retry.retry(check_ready, limit=self._domain_config.global_config.common.retryCommonWaitTimes, check_msg="Wait havenask cluster ready")
        return succ
    
    def get_status(self, **kwargs):
        if not self.is_ready():
            Logger.error("Havenask admin not ready")
            return None
            
        scheduler_service = self.get_scheduler_service()
        processors_status_map = scheduler_service.get_processors_status()
        workers_status_map = self._master.get_containers_status()
        
        status_map = {"admin": [], "database": {}, "qrs": {}}
        if len(processors_status_map) != 0:
            for group, role_map in processors_status_map.items():
                for role, role_status in role_map.items():
                    if role not in status_map:
                        status_map[group][role] = []
            
            for key, worker_status_list in workers_status_map.items():
                for worker_status in worker_status_list:
                    if worker_status.role == "admin":
                        processor_status = ClusterProcessorStatus.from_hippo_worker_info(worker_status)
                        processor_status.processorName = self._global_config.get_master_command(self._key)
                        processor_status.processorStatus = ProcessorStatusType.RUNNING
                        status_map["admin"].append(processor_status)
                    else:
                        is_find = False
                        group, role = worker_status.role.split(".")
                        role_status = processors_status_map[group][role]
                        for processor_status in role_status.processorList:
                            if processor_status.ip == worker_status.ip and processor_status.role == role:
                                processor_status.merge_from_hippo_worker_info(worker_status)
                                processor_status.processorName = self._global_config.get_worker_command(HapeCommon.HAVENASK_KEY)
                                is_find = True
                                status_map[group][role].append(processor_status)
                                break
                        if not is_find:
                            processor_status = ShardGroupProcessorStatus.from_hippo_worker_info(worker_status)
                            status_map[group][role].append(processor_status)
            
        is_cluster_ready = True
        hint = ""
        for group, role_map in status_map.items():
            if group == "admin":
                continue
            if len(role_map) == 0:
                is_cluster_ready = False
                hint = "Group {} has no containers, maybe lack of candidate nodes".format(group)
                break
            for role, node_list in role_map.items():
                if len(node_list) == 0:
                    is_cluster_ready = False
                    hint = "Group {} Role {} has no containers, maybe lack of candidate nodes".format(group, role)
                    break
                
                if processors_status_map[group][role].readyForCurVersion == False:
                    is_cluster_ready = False
                    hint = "Group {} Role {} has unready containers, or mabye lack of candidate nodes".format(group, role)
                    break
            
        
        cluster_status = ShardGroupClusterStatus(
            serviceZk = self._global_config.get_service_appmaster_zk_address(self._key),
            hippoZk = self._global_config.get_service_hippo_zk_address(self._key),
            sqlClusterInfo = status_map,
            leaderAddress = self.get_leader_http_address(),
            hint = hint,
            clusterStatus = "READY" if is_cluster_ready else "NOT_READY"
        )
        return attr.asdict(cluster_status)
    
    
    def get_ready_tables(self):
        ## collect tables ready in qrs
        qrs_worker_ips, tables_collect = self.collect_qrs_tables()
        ready_tables = set()
        for table in tables_collect:
            if len(tables_collect[table]) == len(qrs_worker_ips):
                ready_tables.add(table)
                
        ## check tables all ready in database
        result = []
        status = self.get_status()["sqlClusterInfo"]
        for table in ready_tables:
            expect_searcher_shard_count = self.catalog_manager.get_table_shard_count(table)
            expect_searcher_counts = self._global_config.havenask.searcherReplicaCount * expect_searcher_shard_count
            ready_counts = 0
            for role, processor_status_list in status["database"].items():
                for processor_status in processor_status_list:
                    if processor_status['signature'].find(table+".") != -1:
                        ready_counts += 1
            if ready_counts == expect_searcher_counts:
                result.append(table)
            else:
                Logger.debug("Table {} expect ready searcher count {}, real ready searcher count {}".format(table, expect_searcher_counts, ready_counts))
                        
        return result
    
    def collect_qrs_tables(self):
        qrs_worker_ips = self.get_qrs_ips()
        if qrs_worker_ips == None:
            return [], {}
        
        Logger.debug("Start to collect table in all qrs")
        Logger.debug("Qrs iplist: {}".format(qrs_worker_ips))
        
        qrshttpPort = self._global_config.havenask.qrsHttpPort
        
        tables_collect = {}
        for ip in qrs_worker_ips:
            try:
                response = requests.post("http://{}:{}/sqlClientInfo".format(ip, qrshttpPort))
                data = response.json()
                tables = data['result']["default"][HapeCommon.DEFAULT_DATABASE]['tables']
                for table in tables:
                    if table.endswith("summary_"):
                        continue
                    if table not in tables_collect:
                        tables_collect[table] = []
                    tables_collect[table].append(ip)  
            except Exception as e:
                # Logger.info(traceback.format_exc())
                pass
            
        return qrs_worker_ips, tables_collect
    
    
    def get_qrs_ips(self):
        qrs_count = self._global_config.havenask.qrsReplicaCount
                
        def check_qrs_replicas():
            qrs_worker_ips = self.parse_qrs_ip()
            if len(qrs_worker_ips) != qrs_count:
                Logger.info("Qrs expect to have {} replicas started, now {}".format(qrs_count, len(qrs_worker_ips)))
                return False
            else:
                return True
        check_msg = "qrs achieve target replicas"
        ready = Retry.retry(check_qrs_replicas, check_msg, 50)
        if not ready:
            return None
        else:
            return self.parse_qrs_ip()
        
    def parse_qrs_ip(self):
        result = []
        workers_status_map = self._master.get_containers_status()
        for role in workers_status_map:
            if role.startswith("qrs"):
                for worker_status in workers_status_map[role]:
                    result.append(worker_status.ip)
        return result