from hape.utils.logger import Logger
from hape.domains.admin.events.event_handler import EventHandler
from hape.common.constants.hape_common import HapeCommon


'''
deploy package
'''
class DpHandler(EventHandler):
    
    def name(self):
        return HapeCommon.DP_COMMAND
    
    def watch_command(self):
        return HapeCommon.DP_COMMAND
    
    def handle(self, domain_name, role_name, worker_name, user_target):
        final_target = self._domain_heartbeat_service.read_worker_final_target(domain_name, role_name, worker_name)
        final_target["plan"][HapeCommon.PROCESSOR_INFO_KEY]["packages"] = user_target["plan"][HapeCommon.PROCESSOR_INFO_KEY]["packages"]
        final_target["user_cmd"] = HapeCommon.DP_COMMAND
        Logger.info("dp target {}".format(final_target))
        self._domain_heartbeat_service.write_worker_final_target(domain_name, role_name, worker_name, final_target)
        self._domain_heartbeat_service.distribute_target(final_target)
        self._domain_heartbeat_service.remove_worker_user_target(domain_name, role_name, worker_name)
        Logger.info("user target is consumed")
        return True
