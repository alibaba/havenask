from hape.utils.logger import Logger
from hape.domains.target import Target
from hape.domains.admin.events.event_handler import EventHandler
from hape.common.container.container_service import *
from hape.common import HapeCommon


'''
stop worker
'''
class StopHandler(EventHandler,object):
    def __init__(self, global_conf):
        super(StopHandler, self).__init__(global_conf)
        self._container_service = ContainerServiceFactory.create(self._global_conf)
        
    def handle(self, domain_name, role_name, worker_name, user_target):
        if user_target != None:
            final_target = user_target
            final_target["type"] = Target.FINAL_TARGET
        else:
            final_target = self._domain_heartbeat_service.read_worker_heartbeat(domain_name, role_name, worker_name)    
            
        if final_target != None:
            final_target["user_cmd"] = HapeCommon.STOP_WORKER_COMMAND
            self._stop_worker(final_target)
            self._domain_heartbeat_service.write_worker_final_target(domain_name, role_name, worker_name, final_target)
            self._domain_heartbeat_service.remove_worker_targetdir(domain_name, role_name, worker_name, stop_mode=True)
        return True
        
    def watch_command(self):
        return HapeCommon.STOP_WORKER_COMMAND
    
    def name(self):
        return HapeCommon.STOP_WORKER_COMMAND
    
    def _stop_worker(self, final_target):
        Logger.info("submmit stop worker request to container service")
        self._container_service.stop_worker(final_target["plan"][HapeCommon.PROCESSOR_INFO_KEY])
        