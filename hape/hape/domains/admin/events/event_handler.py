from hape.domains.target import *
from hape.utils.logger import Logger

class EventHandler:
    def __init__(self, global_conf):
        self._global_conf = global_conf
        self._domain_heartbeat_service = DomainHeartbeatService(global_conf)
    
    def handle(self, domain_name, role_name, worker_name, user_target):
        raise NotImplementedError
    
    def watch_command(self):
        raise NotImplementedError
    
    def name(self):
        raise NotImplementedError