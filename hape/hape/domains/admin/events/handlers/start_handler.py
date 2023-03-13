from hape.utils.shell import Shell
from hape.utils.logger import Logger
from hape.domains.target import Target
from hape.domains.admin.events.event_handler import EventHandler
from hape.common.container.container_service import *
from hape.common import HapeCommon


'''
start worker
'''
class StartHandler(EventHandler, object):

    def __init__(self, global_conf):
        super(StartHandler, self).__init__(global_conf)
        self._container_service = ContainerServiceFactory.create(self._global_conf)
        
    def handle(self, domain_name, role_name, worker_name, user_target):
        final_target = user_target
        final_target["type"] = Target.FINAL_TARGET
            
        self._domain_heartbeat_service.write_worker_final_target(domain_name, role_name, worker_name, final_target)
        self._start_worker(final_target)
        self._domain_heartbeat_service.distribute_target(final_target)
        self._domain_heartbeat_service.remove_worker_user_target(domain_name, role_name, worker_name)
        Logger.info("user target is consumed")
        return True
        
    def _start_worker(self, final_target):
        Logger.info("submmit start worker request to container service")
        self._container_service.start_worker(final_target["host_init"], final_target["plan"][HapeCommon.PROCESSOR_INFO_KEY])

        
    def watch_command(self):
        return HapeCommon.START_WORKER_COMMAND
    
    def name(self):
        return HapeCommon.START_WORKER_COMMAND