from hape.utils.shell import Shell
from hape.utils.logger import Logger
from hape.domains.target import Target
from hape.domains.admin.events.event_handler import EventHandler
from hape.common.container.container_service import *
from hape.common import HapeCommon
import time

class AutoKeepWorkerHandler(EventHandler, object):
    def __init__(self, global_conf):
        super(AutoKeepWorkerHandler, self).__init__(global_conf)
        self._container_service = ContainerServiceFactory.create(self._global_conf)
        
    def handle(self, domain_name, role_name, worker_name, user_target):
        final_target = self._domain_heartbeat_service.read_worker_final_target(domain_name, role_name, worker_name)
        heartbeat = self._domain_heartbeat_service.read_worker_heartbeat(domain_name, role_name, worker_name)["plan"]
        

        if heartbeat == None or "status" not in heartbeat or (heartbeat["status"] != "running" and heartbeat["status"] != "finished"):  
            return False
        
        if "timestamp" in heartbeat and (int(time.time()) - int(heartbeat["timestamp"])) < 180 :
            return False
        Logger.info("worker {} heartbeat timeout, will restart".format(worker_name))
            
        
        final_target["type"] = Target.FINAL_TARGET
        final_target["user_cmd"] = HapeCommon.START_WORKER_COMMAND
            
        self._domain_heartbeat_service.write_worker_final_target(domain_name, role_name, worker_name, final_target)
        self._start_worker(final_target)
        self._domain_heartbeat_service.distribute_target(final_target)
        return True
        
    def _start_worker(self, final_target):
        Logger.info("submmit start worker request to container service")
        self._container_service.start_worker(final_target["host_init"], final_target["plan"][HapeCommon.PROCESSOR_INFO_KEY])

            
    def watch_command(self):
        return None
    
    def name(self):
        return "auto-keep-worker"