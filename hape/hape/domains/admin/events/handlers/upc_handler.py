from hape.utils.logger import Logger
from hape.domains.admin.events.event_handler import EventHandler
from hape.common.container.container_service import *
from hape.common.constants.hape_common import HapeCommon

'''
update config
'''
class UpcHandler(EventHandler, object):
    def __init__(self, global_conf):
        super(UpcHandler, self).__init__(global_conf)
        self._container_service = ContainerServiceFactory.create(self._global_conf)
    
    
    def name(self):
        return HapeCommon.UPC_COMMAND
    
    def watch_command(self):
        return HapeCommon.UPC_COMMAND
    
    def handle(self, domain_name, role_name, worker_name, user_target):
        final_target = self._domain_heartbeat_service.read_worker_final_target(domain_name, role_name, worker_name)
        final_target["plan"][HapeCommon.ROLE_BIZ_PLAN_KEY[role_name]]["config_path"] = user_target["plan"][HapeCommon.ROLE_BIZ_PLAN_KEY[role_name]]["config_path"]
        final_target["user_cmd"] = HapeCommon.UPC_COMMAND
        Logger.info("upc target {}".format(final_target))
        ## if there is a updater before
        self._container_service.worker_execute(final_target["plan"][HapeCommon.PROCESSOR_INFO_KEY], "pkill -f .*part_search_update.py.*")
        self._domain_heartbeat_service.write_worker_final_target(domain_name, role_name, worker_name, final_target)
        self._domain_heartbeat_service.distribute_target(final_target)
        user_target = self._domain_heartbeat_service.read_worker_user_target(domain_name, role_name, worker_name)
        ## avoid remove new target coming from users
        if user_target != None and user_target["user_cmd"] == HapeCommon.UPC_COMMAND:
            self._domain_heartbeat_service.remove_worker_user_target(domain_name, role_name, worker_name)
        Logger.info("user target is consumed")
        return True
