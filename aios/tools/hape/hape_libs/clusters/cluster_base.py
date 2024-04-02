import os
import json
import attr

from hape_libs.utils.logger import Logger
from hape_libs.utils.retry import Retry
from hape_libs.appmaster import *
from hape_libs.config.domain_config import DomainConfig
    
class ProcessorStatusType:
    UNKNOWN = "UNKNOWN"
    RUNNING = "RUNNING"
    NOT_RUNNING = "NOT_RUNNING"

@attr.s
class ClusterProcessorStatus(object):
    ip = attr.ib(default="UNKNOWN")
    role = attr.ib(default="UNKNOWN")
    containerName = attr.ib(default="UNKNOWN")
    processorStatus = attr.ib(default=ProcessorStatusType.UNKNOWN)
    containerStatus = attr.ib(default=HippoContainerStatusType.UNKNOWN)
    processorName = attr.ib(default="UNKNOWN")
    
    @staticmethod
    def from_hippo_worker_info(hippo_worker_info):
        return ClusterProcessorStatus(
            ip=hippo_worker_info.ip,
            role=hippo_worker_info.role,
            containerName=hippo_worker_info.containerName,
            containerStatus=hippo_worker_info.containerStatus,
        )
    
    def merge_from_hippo_worker_info(self, hippo_worker_info):
        self.ip = hippo_worker_info.ip
        self.role = hippo_worker_info.role
        self.containerName = hippo_worker_info.containerName
        self.containerStatus = hippo_worker_info.containerStatus

@attr.s
class ClusterStatus(object):
    leaderAddress = attr.ib(default="UNKNOWN")
    processors = attr.ib(default = [])
    serviceZk = attr.ib(default="UNKNOWN")
    hippoZk = attr.ib(default="UNKNOWN")
    
    
@attr.s
class ShardGroupProcessorStatus(ClusterProcessorStatus):
    replicaId = attr.ib(default="UNKNOWN")
    workerStatus = attr.ib(default="UNKNOWN")
    signature = attr.ib(default="UNKNOWN")
    targetSignature = attr.ib(default="UNKNOWN")
    readyForCurVersion = attr.ib(default=False)
    
    @staticmethod
    def from_hippo_worker_info(hippo_worker_info):
        return ShardGroupProcessorStatus(
            ip=hippo_worker_info.ip,
            role=hippo_worker_info.role,
            containerName=hippo_worker_info.containerName,
            containerStatus=hippo_worker_info.containerStatus,
        )
    def merge_from_hippo_worker_info(self, hippo_worker_info):
        self.ip = hippo_worker_info.ip
        self.role = hippo_worker_info.role
        self.containerName = hippo_worker_info.containerName
        self.containerStatus = hippo_worker_info.containerStatus

@attr.s
class ShardGroupRoleStatus(object):
    processorList = attr.ib(default = [])
    readyForCurVersion = attr.ib(default=False)
    
@attr.s
class ShardGroupClusterStatus(object):
    leaderAddress = attr.ib(default="UNKNOWN")
    serviceZk = attr.ib(default="UNKNOWN")
    serviceZk = attr.ib(default="UNKNOWN")
    hippoZk = attr.ib(default="UNKNOWN")
    clusterStatus = attr.ib(default="UNKNOWN")
    sqlClusterInfo = attr.ib(default={})
    hint = attr.ib(default="")
    
class ClusterBase(object):
    def __init__(self, key, domain_config): #type: (str, DomainConfig)->None
        self._key = key
        self._domain_config = domain_config
        self._global_config = domain_config.global_config
        self._master = HippoAppMasterBase.create(key, domain_config)
        
    def start(self, allow_restart=False, only_admin=False):
        ## before start master
        Logger.info("Begin to start {} admin".format(self._key))
        is_ready = self.is_ready()
        if is_ready and not allow_restart:
            Logger.error("{} admin already exists, please execute `hape stop` or `hape restart` first".format(self._key.capitalize()))
            return False
        
        if is_ready and allow_restart:
            Logger.info("{} admin already exists, will restart".format(self._key))
            self.stop(only_admin=only_admin)
        
        raw_hippo_plan = self._domain_config.template_config.get_rendered_appmaster_plan(self._key)
        hippo_plan = HippoMasterPlan(**raw_hippo_plan)
        self.before_master_start(hippo_plan)
        
        ## start master
        succ = self._master.start(hippo_plan)
        if not succ:
            return False
        
        check_msg = "{} admin is ready".format(self._key.capitalize())
        succ = Retry.retry(lambda: self.is_ready(), check_msg, self._global_config.common.retryCommonWaitTimes)
        if not succ:
            return False
        
        ## after start master
        self.after_master_start()
        return True
    
    ## prepare necessary resources before starting master
    def before_master_start(self, hippo_plan): #type:(str, HippoMasterPlan) -> None
        return True
    
    ## create restful resources on master
    def after_master_start(self):
        return True
    
    def stop(self, is_delete=False, only_admin=False):
        self._master.stop(is_delete=is_delete, only_admin=only_admin)
    
    def is_ready(self):
        raise NotImplementedError
    
    def get_leader_http_address(self):
        address = self.get_leader_ip_address()
        if address == None:
            return None
        else:
            return "http://" + address
    
    def get_leader_ip_address(self):
        raise NotImplementedError
    
    def get_status(self, **kwargs):
        raise NotImplementedError


    
