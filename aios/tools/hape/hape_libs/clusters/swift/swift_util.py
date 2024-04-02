import attr
import requests
import traceback

from hape_libs.clusters.cluster_base import *
from hape_libs.utils.logger import Logger
from hape_libs.utils.retry import Retry
from hape_libs.clusters import *
from hape_libs.utils.shell import LocalShell


class SwiftToolWrapper():
    def __init__(self, binary_path, swift_appmaster_zk):
        self._binary_path = binary_path
        self._swift_appmaster_zk = swift_appmaster_zk
        
    def deploy_config(self, config_path):
        argstr = "dp -z {} -l {}".format(self._swift_appmaster_zk, config_path)
        out, flag = self.wrapped_execute(argstr, grep_text="success")
        
        if flag:
            hint = "max version is "
            offset = out.find(hint)
            if offset != -1:
                version = out[offset + len(hint):]
                Logger.info("Deploy swift config succeed")
                return version.strip(), True
        else:
            Logger.error("Failed to execute swift tools dp command, detail: [{}]".format(out))
            
        Logger.error("Deploy swift config failed")
        return "", False
        
    def add_topic(self, topic_name, partiton_count):
        
        argstr = "at -d -z {} -t {} -c {} -y 1024 -r 30 -m security_mode".format(self._swift_appmaster_zk, topic_name, partiton_count)
        add_out, succ = self.wrapped_execute(argstr, grep_text="success")
        if not succ:
            Logger.warning("Add swift Topic response: {}".format(add_out))
        
        def _try_add_topic():
            argstr = "gti -z {} -t {}".format(self._swift_appmaster_zk, topic_name)
            status_out, succ = self.wrapped_execute(argstr, grep_text="TopicStatus: RUNNING")
            Logger.info("Get swift topic status Response: {}".format(status_out))
            Logger.info("Swift Topic status is not ready")
            return succ
        
        check_msg = "swift topic created and ready"
        return Retry.retry(_try_add_topic, check_msg)
    
    def delete_topic(self, topic_name):
        argstr = "dt -z {} -t {}".format(self._swift_appmaster_zk, topic_name)
        out = self.wrapped_execute(argstr)
        succ = (out.find("success") != -1 or out.find("ERROR_ADMIN_TOPIC_NOT_EXISTED") != -1)
        if not succ:
            Logger.warning("Failed to delete swift topic, detail: {}".format(out))
    
    def check_admin_alive(self):
        argstr = "gs -z {} ".format(self._swift_appmaster_zk)
        out, alive = self.wrapped_execute(argstr, grep_text="Status[ALIVE]")
        return alive
    
    def get_partition_count(self, topic_name):
        info = self.wrapped_execute("gettopicinfo -z {} -t {}".format(self._swift_admin_processor.service_zk_path(), topic_name))
        for line in info.split("\n"):
            if ":" in line:
                splits = line.split(":")
                key = splits[0]
                if key == "PartitionCount":
                    return int(":".join(splits[1:]))
                
                
    def stop_swift(self):
        info = self.wrapped_execute("stop -z {}".format(self._swift_appmaster_zk))
     
    def wrapped_execute(self, argstr, grep_text = None):
        envs = [
            "PYTHONPATH={}/usr/local/lib/python/site-packages:$PYTHONPATH".format(self._binary_path),
            "PATH={}/usr/local/bin:$PATH".format(self._binary_path),
        ]
        cmd = " ".join(envs) + " swift " + argstr
        return LocalShell.execute_command(cmd, grep_text = grep_text)

class SwiftAdminService:
    def __init__(self, http_address):
        self._http_address = http_address
        
    def get_all_processor_status(self):
        address = "{}/Controller/getAllWorkerStatus".format(self._http_address)
        processor_status_map = {}
        data = {}
        try:
            response = requests.get(address, timeout=2)
            data = response.json()
            Logger.debug("swift admin get all worker status response: {}".format(data))
        except:
            Logger.warning("Failed to get cluster status by api: {}".format(address))
            return processor_status_map

        if data.get('errorInfo', {}).get('errMsg', None) != 'ERROR_NONE' or 'workers' not in data:
            return processor_status_map
        
        for raw_processor_status in data['workers']:
            current = raw_processor_status.get('current', {})
            address = current.get('address', None)
            if address ==  None:
                continue
            
            ip = address.split("#")[-1].split(":")[0]
            role = "#".join(address.split("#")[:-2])

            if role == None:
                continue
            
            decisionStatus = raw_processor_status.get('decisionStatus', None)
            if decisionStatus == None:
                continue
            
            status =  ProcessorStatusType.RUNNING  if decisionStatus == 'ROLE_DS_ALIVE' else ProcessorStatusType.NOT_RUNNING
            
            if role not in processor_status_map:
                processor_status_map[role] = []
                
            processor_status_map[role].append(
                ClusterProcessorStatus(
                    ip = ip, 
                    role = role, 
                    processorStatus = status
            ))
        return processor_status_map
            
            
        
        
    
    

    