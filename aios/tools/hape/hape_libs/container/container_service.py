import os
import getpass
from hape_libs.common import HapeCommon

class ContainerMeta:
    def __init__(self, name, ip):
        self.name = name
        self.ip = ip

class ContainerSpec:
    def __init__(self, image, cpu, mem):
        self.image = image
        self.cpu = cpu
        self.mem = mem
        
class ProcessorSpec:
    def __init__(self, envs, args, command):
        self.envs = envs
        self.args = args
        self.command = command


class ContainerServiceBase():
    def __init__(self, cluster_config):
        self._current_homedir = cluster_config.get_default_var("userHome")
        self._process_workdir_root = cluster_config.get_domain_config_param(HapeCommon.COMMON_CONF_KEY, "processorWorkdirRoot")
        self._user = getpass.getuser()
        self._uid = os.getuid()
        self._gid = os.getgid()
        ## only for test
        self._processor_tag = cluster_config.get_default_var("processorTag")

    def start_process(self, container_meta, processor_spec):
        raise NotImplementedError

    def create_container(self, container_meta, container_spec):
        raise NotImplementedError

    def stop_container(self, container_meta, processor_cmd):
        raise NotImplementedError
    
    def execute_command(self, container_meta, process_spec, grep_text = None):
        raise NotImplementedError