# -*- coding: utf-8 -*-

import os
import socket
import getpass
import json
from six.moves.configparser import ConfigParser

from hape_libs.common import *
from hape_libs.utils.template import *
from hape_libs.utils.shell import * 
from .template_config import TemplateConfig


def _get_hape_root():
    here = os.path.dirname(os.path.realpath(__file__))
    candidates = [
        "../../",
        "../../../../../../../"  ## when hape_libs in usr/local/lib/python/site-packages
    ]
    file = "hape"
    for path in candidates:
        hape_root = os.path.realpath(os.path.join(here, path))
        full_path = os.path.realpath(os.path.join(here, path, file))
        if os.path.exists(full_path):
            Logger.info("find hape.py under {}, set as hape root".format(hape_root))
            return hape_root
    return hape_root

def _get_local_host():
    try:
        # 获取本机的主机名
        hostname = socket.gethostname()
        # 使用主机名获取本机的 IPv4 地址
        local_ip = socket.gethostbyname(hostname)
    except socket.error:
        # 如果获取失败，返回默认地址
        local_ip = "127.0.0.1"
    return local_ip


default_variables = {
    "user": getpass.getuser(),
    "userHome": os.path.expanduser("~"),
    "hapeRoot": _get_hape_root(),
    "localHost": _get_local_host()
}        


class ClusterConfig():
    def __init__(self, config_dir = None):
        self.config_dir =  config_dir
        if self.config_dir == None:
            self.config_dir = os.path.join(default_variables["hapeRoot"], "hape_conf/default")
        else:
            self.config_dir = os.path.realpath(self.config_dir)
            
        self.default_variables = default_variables
        self.domain_config = ConfigParser()
        self.domain_config.optionxform = str
        Logger.info("Hape config directory: {}".format(self.config_dir))
        self.domain_config.read(os.path.join(self.config_dir, "global.conf"))
        self.domain_zk_set = False
        self.swift_zk_set = False
        
    def init(self):
        self._fill_param()
        
        for key in self.domain_config.sections():
            for subkey, value in self.domain_config.items(key):
                self.domain_config.set(key, subkey, TemplateRender.render(value, self.default_variables))

        domain_config_dict = {s:dict(self.domain_config.items(s)) for s in self.domain_config.sections()}
        Logger.info("Cluster config: {}".format(json.dumps(domain_config_dict, indent=4)))
        self._render_template()
        self._validate()
        
    def _validate(self):
        pass
        
    def _fill_param(self):
        self.fill_domain_config_param_na(HapeCommon.COMMON_CONF_KEY, "hadoopHome", "/usr/local/hadoop/hadoop/")
        self.fill_domain_config_param_na(HapeCommon.COMMON_CONF_KEY, "javaHome", "/opt/taobao/java")
        default_domain_zk = "zfs://{}:2181/havenask".format(self.default_variables["localHost"])
        if self.has_domain_config_param(HapeCommon.COMMON_CONF_KEY, "domainZkRoot"):
            self.domain_zk_set = True
        else:
            self.fill_domain_config_param_na(HapeCommon.COMMON_CONF_KEY, "domainZkRoot", default_domain_zk)
            self.domain_zk_set = False
        
        
        for path_key in ["dataStoreRoot", "binaryPath"]:
            raw_path = self.get_domain_config_param(HapeCommon.COMMON_CONF_KEY, path_key)
            raw_path = TemplateRender.render(raw_path, self.default_variables)
            path = self._convert_path(raw_path)
            self.set_domain_config_param(HapeCommon.COMMON_CONF_KEY, path_key, path)
                
        self.fill_domain_config_param_na(HapeCommon.COMMON_CONF_KEY, "processorMode", "docker")
        self.fill_domain_config_param_na(HapeCommon.COMMON_CONF_KEY, "processorWorkdirRoot", self.default_variables["userHome"])
        for key in [HapeCommon.SWIFT_KEY, HapeCommon.HAVENASK_KEY, HapeCommon.BS_KEY]:
            if self.domain_config.has_section(key):
                # self.fill_domain_config_param_na(key, "image", image)
                self.fill_domain_config_param_na(key, "adminIpList", self.default_variables['localHost'])
            
        self.fill_domain_config_param_na(HapeCommon.SWIFT_KEY, "workerIpList", self.default_variables['localHost'])
        self.fill_domain_config_param_na(HapeCommon.HAVENASK_KEY, "qrsIpList", self.default_variables['localHost'])
        self.fill_domain_config_param_na(HapeCommon.HAVENASK_KEY, "searcherIpList", self.default_variables['localHost'])
        self.fill_domain_config_param_na(HapeCommon.HAVENASK_KEY, "allowMultiSlotInOne", "false")
        self.set_default_var("processorTag", None)
        
        
        if self.domain_config.has_section(HapeCommon.BS_KEY):
            self.fill_domain_config_param_na(HapeCommon.BS_KEY, "workerIpList", self.default_variables['localHost'])
        
        if self.has_domain_config_param(HapeCommon.SWIFT_KEY, "swiftZkRoot"):
            self.swift_zk_set = True
        else:
            default_swift_zk = self.get_domain_config_param(HapeCommon.COMMON_CONF_KEY, "domainZkRoot")
            self.fill_domain_config_param_na(HapeCommon.SWIFT_KEY, "swiftZkRoot", default_swift_zk)
            self.swift_zk_set = False
        
    def _render_template(self):
        render_vars = {}
        render_vars.update(default_variables)
        for key in self.domain_config.sections():
            for subkey, value in self.domain_config.items(key):
                render_vars[key+"."+subkey] = value
                
        self.template_config = TemplateConfig(self, render_vars)
        self.template_config.render()
    
    def set_default_var(self, key, value):
        self.default_variables[key] = value    
        
    def get_default_var(self, key):
        return self.default_variables[key]
    
    def get_domain_config_param(self, key, subkey):
        return self.domain_config.get(key, subkey, vars=None)
    
    def set_domain_config_param(self, key, subkey, value):
        return self.domain_config.set(key, subkey, value)
    
    def has_domain_config_param(self, key, subkey):
        return self.domain_config.has_option(key, subkey)
    
    def fill_domain_config_param_na(self, key, subkey, value):
        if self.has_domain_config_param(key, subkey) == False:
            self.set_domain_config_param(key, subkey, value)
    
    def get_admin_ip_list(self, key):
        return self.get_domain_config_param(key, "adminIpList").split(';')  
    
    def get_worker_ip_list(self, key):
        if key != HapeCommon.HAVENASK_KEY:
            return self.get_domain_config_param(key, "workerIpList").split(';')
        else:
            return self.get_domain_config_param(key, "qrsIpList").split(";") + self.get_domain_config_param(key, "searcherIpList").split(";")
            
    def set_domain_zk_root(self, zk_root):
        self.domain_config.set(HapeCommon.COMMON_CONF_KEY, "domainZkRoot", zk_root)
        
    def set_processor_mode(self, mode):
        self.domain_config.set(HapeCommon.COMMON_CONF_KEY,"processorMode", mode)
        
    def set_binary_path(self, install_root):
        self.domain_config.set(HapeCommon.COMMON_CONF_KEY,"binaryPath", install_root)
        
    def get_binary_path(self):
        return self.domain_config.get(HapeCommon.COMMON_CONF_KEY,"binaryPath")
        
    def set_admin_http_port(self, port):
        self.domain_config.set(HapeCommon.HAVENASK_KEY,"adminHttpPort", port)
        
    def _convert_path(self, path):
        if path.startswith("hdfs://") or path.startswith("/"):
            return path
        else:
            return os.path.realpath(os.path.join(default_variables["hapeRoot"], path))
        
    def has_bs_domain_config(self):
        return self.domain_config.has_section(HapeCommon.BS_KEY)
    
    def get_data_store_root(self):
        return os.path.join(self.get_domain_config_param(HapeCommon.COMMON_CONF_KEY, "dataStoreRoot"))
    
    def set_data_store_root(self, path):
        return self.set_domain_config_param(HapeCommon.COMMON_CONF_KEY, "dataStoreRoot", path)
    
    def get_suez_cluster_store_root(self):
        return os.path.join(self.get_data_store_root(), "suez_cluster_store")

    def get_db_store_root(self):
        return os.path.join(self.get_data_store_root(), "database_store")
    
    def get_templates_root(self):
        return os.path.join(self.get_data_store_root(), "templates_store")
        
    def set_processor_workdir(self, path):
        return self.set_domain_config_param(HapeCommon.COMMON_CONF_KEY, "processorWorkdirRoot", path)
        
    
    
