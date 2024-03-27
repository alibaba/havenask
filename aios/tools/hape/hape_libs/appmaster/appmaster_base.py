import os
import attr
import json

from .appmaster_hippo_common import *
from hape_libs.utils.logger import Logger
from hape_libs.utils.fs_wrapper import FsWrapper
from hape_libs.config import *
    
class HippoAppMasterBase(object):
    def __init__(self, key, domain_config): #type:(str, DomainConfig)->None
        self._key = key
        self._domain_config = domain_config
        self._global_config = domain_config.global_config
        self.service_name = self._global_config.get_appmaster_service_name(key)
        self.zk_addr = self._global_config.get_service_appmaster_zk_address(key)
        self.admin_zk_fs_wrapper = FsWrapper(self.zk_addr)
    
    PROCESSOR_MODE_TO_CLS = {}
    @staticmethod
    def register_master_cls(processor_mode, master_cls):
        Logger.info("register cluster master type: {}".format(processor_mode))
        HippoAppMasterBase.PROCESSOR_MODE_TO_CLS[processor_mode] = master_cls
    
    @staticmethod
    def create(cluster_role_key, domain_config):
        processor_mode = domain_config.global_config.common.processorMode
        if processor_mode in HippoAppMasterBase.PROCESSOR_MODE_TO_CLS:
            return HippoAppMasterBase.PROCESSOR_MODE_TO_CLS[processor_mode](cluster_role_key, domain_config)
        else:
            raise ValueError("processor mode [{}] not supported".format(processor_mode))
    
    def start(self, master_plan): #type:(HippoMasterPlan) -> bool
        raise NotImplementedError
    
    def stop(self, is_delete = False):
        raise NotImplementedError
    
    def get_containers_status(self): #type:() -> dict[HippoAppContainerInfo]
        raise NotImplementedError
    
