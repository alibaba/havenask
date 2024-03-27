import attr
import json
import os

from hape_libs.utils.fs_wrapper import FsWrapper
from hape_libs.utils.logger import Logger

@attr.s
class AppMasterResource(object):
    amount = attr.ib(default=attr.NOTHING)
    name = attr.ib(default=attr.NOTHING)
    type = attr.ib(default=attr.NOTHING)
 
@attr.s
class AppMasterPackageInfo(object):
    packageURI = attr.ib(default=attr.NOTHING)
    type = attr.ib(default=attr.NOTHING)

@attr.s
class KeyValuePair(object):
    key = attr.ib(default=attr.NOTHING)
    value = attr.ib(default=attr.NOTHING)

@attr.s
class AppMasterProcesses(object):
    args = attr.ib(converter=lambda arg_list: [KeyValuePair(**arg) for arg in arg_list]) #type: list[KeyValuePair]
    envs = attr.ib(converter=lambda env_list: [KeyValuePair(**env) for env in env_list]) #type: list[KeyValuePair]
    cmd = attr.ib(default=attr.NOTHING)
    isDaemon = attr.ib(default=attr.NOTHING)
    processName = attr.ib(default=attr.NOTHING)
   
@attr.s
class AppMasterProcessContext(object):
    requiredPackages = attr.ib(converter=lambda rs: [AppMasterPackageInfo(**r) for r in rs]) #type: list[AppMasterPackageInfo]
    processes = attr.ib(converter=lambda rs: [AppMasterProcesses(**r) for r in rs]) #type: list[AppMasterProcesses]
    @requiredPackages.validator
    def requiredPackages_validator(self, attribute, value):
        if len(value) != 1:
            raise ValueError("length of requiredPackages must be 1")
    
    @processes.validator
    def processes_validator(self, attribute, value):
        if len(value) != 1:
            raise ValueError("length of processes must be 1")
    
@attr.s
class HippoMasterPlan(object):
    resources  = attr.ib(converter=lambda rs: [AppMasterResource(**r) for r in rs]) #type: AppMasterResource
    processLaunchContext = attr.ib(converter=lambda d: AppMasterProcessContext(**d)) #type: AppMasterProcessContext
    
@attr.s
class HippoContainerStatusType(object):
    UNKNOWN = "UNKNOWN"
    RUNNING = "RUNNING"
    NOT_RUNING = "NOT_RUNING"
@attr.s 
class HippoAppContainerInfo(object):
    ip = attr.ib(default="UNKNOWN")
    role = attr.ib(default="UNKNOWN")
    containerName = attr.ib(default="UNKNOWN")
    containerStatus = attr.ib(default=HippoContainerStatusType.UNKNOWN)

def prepare_candidate_ips(hippo_zk_address, candidate_ips): 
    candidate_path = "candidate_ips"
    hippo_zk_fs_wrapper = FsWrapper(hippo_zk_address)
    if hippo_zk_fs_wrapper.exists(candidate_path):
        Logger.info("Candidate ips zk path {} already exists, will not recreate it".format(hippo_zk_fs_wrapper.complete_path(candidate_path)))
        return 
    hippo_zk_fs_wrapper.write(json.dumps(candidate_ips), candidate_path)