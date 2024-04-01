from ctypes import ArgumentError
import attr
import os
import getpass
import socket
import sys

from hape_libs.utils.logger import Logger


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
            Logger.info("find hape under {}, set as hape root".format(hape_root))
            return hape_root
    return hape_root

def _get_local_host():
    try:
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
    except socket.error:
        local_ip = "127.0.0.1"
    return local_ip   
    
    
def _iplist_converter(value):
    return value.split(';') if value else [_get_local_host()]
    
def is_not_none(instance, attribute, value):
    if value is None:
        raise ValueError("The '{}' field is required and cannot be None".format(attribute.name))

@attr.s
class MasterConfigBase(object):
    serviceName = attr.ib(attr.NOTHING)
    image = attr.ib(attr.NOTHING)
    adminCpu = attr.ib(attr.NOTHING)
    adminMem = attr.ib(attr.NOTHING)
    adminIpList = attr.ib(converter = _iplist_converter, default=[])
    k8sNamespace = attr.ib(default=None)
    serviceZkAddr = attr.ib(init=False)
    workerCandidateMap = attr.ib(init=False)
    serviceMasterZkAddr = attr.ib(init=False)
    serviceHippoZkAddr = attr.ib(init=False)


@attr.s
class SwiftConfig(MasterConfigBase):
    workerIpList = attr.ib(converter = _iplist_converter, default=[])
    brokerCount = attr.ib(default=1)
    swiftDataStoreRoot = attr.ib(init=False)
    def __attrs_post_init__(self):
        self.workerCandidateMap = {
            "default": self.workerIpList
        }

@attr.s
class BSConfig(MasterConfigBase):
    workerIpList = attr.ib(converter = _iplist_converter, default=[])
    def __attrs_post_init__(self):
        self.workerCandidateMap = {
            "default": self.workerIpList
        }

@attr.s
class HavenaskConfig(MasterConfigBase):
    allowMultiSlotInOne = attr.ib(default='true')
    qrsIpList = attr.ib(converter = _iplist_converter, default=[])
    searcherIpList = attr.ib(converter = _iplist_converter, default=[])
    adminHttpPort = attr.ib(default=45700)
    qrsHttpPort = attr.ib(default=45800)
    qrsReplicaCount = attr.ib(default=1)
    dbStoreRoot = attr.ib(init=False)
    # enableVirtualIp = attr.ib(default=False)
    suezClusterStoreRoot = attr.ib(init=False)

    def __attrs_post_init__(self):
        self.workerCandidateMap = {
            "qrs": self.qrsIpList,
            "searcher": self.searcherIpList
        }
        
        # if isinstance(self.enableVirtualIp, unicode):
        #     self.enableVirtualIp = self.enableVirtualIp == "true"

@attr.s
class CommonConfig(object):
    binaryPath = attr.ib(attr.NOTHING)
    dataStoreRoot = attr.ib(attr.NOTHING)
    processorMode = attr.ib(default='docker', validator=lambda ins, attri, value: value in {"proc", "docker", "k8s"})
    domainZkRoot = attr.ib(default=None)
    hadoopHome = attr.ib(default='/usr/local/hadoop/hadoop/')
    javaHome = attr.ib(default='/opt/taobao/java')
    kubeConfigPath = attr.ib(default=os.path.expanduser("~/.kube/config"))
    isDomainZkSet = attr.ib(default=False)
    c2K8sNamespace = attr.ib(default="havenask-master")
    c2ClusterName = attr.ib(default="k8s-havenask")
    k8sNamespace = attr.ib(default="havenask-worker")
    catalogName = attr.ib(default="catalog")
    retryCommonWaitTimes = attr.ib(default = 40)
    enableKmonitorPrometheusSink = attr.ib(default = "false")
    kmonitorSinkAddress = attr.ib(default = None)
    kmonitorSinkHost = attr.ib(default="127.0.0.1")
    kmonitorSinkPort = attr.ib(default="9091")
    
    def __attrs_post_init__(self):
        if self.domainZkRoot == None:
            self.domainZkRoot = "zfs://{}:2181/havenask".format(_get_local_host())
            self.isDomainZkSet = False
        else:
            self.isDomainZkSet = True
        if self.processorMode == "k8s":
            self.retryCommonWaitTimes *= 4
            
        if self.kmonitorSinkAddress != None:
            self.enableKmonitorPrometheusSink = 'true'
            splits = self.kmonitorSinkAddress.split(":")
            if len(splits) != 2:
                raise ArgumentError("kmonitorSinkAddress address requires format: <host>:<port>")
            self.kmonitorSinkHost, self.kmonitorSinkPort = splits
        
            
class DefaultVaribles:
    def __init__(self):
        self.user= getpass.getuser()
        self.user_home = os.path.expanduser("~")
        self.local_host = _get_local_host()
        self.hape_root = _get_hape_root()
        
    def to_dict(self):
        return {
            "user": self.user,
            "user_home": self.user_home,
            "local_host": self.local_host,
            "hape_root": self.hape_root
        }
default_variables = DefaultVaribles()
      
@attr.s(init=False) 
class GlobalConfig(object):
    common = attr.ib() # type: CommonConfig
    swift = attr.ib() # type: SwiftConfig 
    bs = attr.ib() # type: BSConfig
    havenask = attr.ib() # type: HavenaskConfig
    default_variables = attr.ib(init=False) # type: DefaultVaribles
    
    def __init__(self, **kwargs):
        required_fields = ['common', 'swift', 'bs', 'havenask']
        missing_fields = [field for field in required_fields if field not in kwargs]
        if missing_fields:
            raise ValueError("Missing required configuration fields: {}".format(', '.join(missing_fields)))
        
        self.common = CommonConfig(**kwargs['common'])
        self.swift = SwiftConfig(**kwargs['swift'])
        self.bs = BSConfig(**kwargs.get('bs', {}))
        self.havenask = HavenaskConfig(**kwargs['havenask'])
        self.fill()
        
    def fill(self):
        self.havenask.dbStoreRoot = os.path.join(self.common.dataStoreRoot, "database_store")
        self.havenask.suezClusterStoreRoot = os.path.join(self.common.dataStoreRoot, "suez_cluster_store")
        self.swift.swiftDataStoreRoot = os.path.join(self.common.dataStoreRoot, "havenask/swift_data_root")
        
        self.default_variables = default_variables
    
        for key in ["bs", "havenask", "swift"]:
            self.add_role_full_zk_path(key)
            
    def add_role_full_zk_path(self, key):
        common_config = self.common
        domainZkRoot = common_config.domainZkRoot
        
        
        service_name = self.__getattribute__(key).__getattribute__("serviceName")
        zk_func_map = {
            "serviceZkAddr": lambda root: os.path.join(root, service_name),
            "serviceMasterZkAddr": lambda root: os.path.join(root, service_name, "appmaster"),
            "serviceHippoZkAddr": lambda root: os.path.join(root, service_name, "hippo"),
        }
        for zk_key, zk_func in zk_func_map.items():
            zk_path = zk_func(domainZkRoot)
            self.__getattribute__(key).__setattr__(zk_key, zk_path)
    
    def get_appmaster_base_config(self, key, subkey):
        config = self.__getattribute__(key)
        service_name = config.__getattribute__(subkey)
        return service_name
    
    def get_service_zk_address(self, key):
        return self.get_appmaster_base_config(key, "serviceZkAddr")
    
    def get_service_appmaster_zk_address(self, key):
        return self.get_appmaster_base_config(key, "serviceMasterZkAddr")
    
    def get_service_hippo_zk_address(self, key):
        return self.get_appmaster_base_config(key, "serviceHippoZkAddr")
    
    def get_appmaster_service_name(self, key):
        return self.get_appmaster_base_config(key, "serviceName")
        
    def get_worker_candidate_maps(self, key):
        return self.get_appmaster_base_config(key, "workerCandidateMap")
    
    def get_admin_candidate_ips(self, key):
        return self.get_appmaster_base_config(key, "adminIpList")
    
    def get_k8s_workers_crds(self, key = None):
        map = {
            "swift": [
                ["CarbonJob", self.common.k8sNamespace, self.default_variables.user + "-" + self.get_appmaster_service_name("swift")]
            ],
            "bs": [
                ["CarbonJob", self.common.k8sNamespace, self.get_appmaster_service_name("bs")]
            ],
            "havenask": [
                ["ShardGroup", self.common.k8sNamespace, "qrs"],
                ["ShardGroup", self.common.k8sNamespace, "database"]
            ]
        }
        if key != None:
            return map[key]
        else:
            return map
            
    def get_k8s_workers_apphash(self, key):
        map = {
            "swift": self.default_variables.user + "_" + self.get_appmaster_service_name(key),
            "bs": self.get_appmaster_service_name(key),
            "havenask": self.get_appmaster_service_name(key)
        }
        return map[key]
        
        
    def get_worker_command(self, key):
        map = {
            "swift": "swift_broker",
            "bs": "build_service_worker",
            "havenask": "ha_sql"
        }
        return map[key]
    
    def get_master_command(self, key):
        map = {
            "swift": "swift_admin",
            "bs": "bs_admin_worker",
            "havenask": "suez_admin_worker"
        }
        return map[key]
    
    
    def get_docker_container_prefix(self, key):
        service_name = self.get_appmaster_service_name(key)
        container_name = "havenask_container" + (("_" + self._global_config.default_variables.user) if key == "swift" else "") + "_" + service_name
        return container_name

        