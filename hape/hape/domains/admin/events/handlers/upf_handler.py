from hape.utils.logger import Logger
from hape.domains.admin.events.event_handler import EventHandler
from hape.common.container.container_service import *
from hape.common.constants.hape_common import HapeCommon

'''
update full index
'''
class UpfHandler(EventHandler, object):
    
    def __init__(self, global_conf):
        super(UpfHandler, self).__init__(global_conf)
        self._container_service = ContainerServiceFactory.create(self._global_conf)
    
    
    
    def name(self):
        return HapeCommon.UPF_COMMAND
    
    def watch_command(self):
        return HapeCommon.UPF_COMMAND
    
    def handle(self, domain_name, role_name, worker_name, user_target):
        if role_name != "searcher":
            return False
        
        user_index_info = user_target["plan"][HapeCommon.ROLE_BIZ_PLAN_KEY[role_name]]["index_info"]
        final_target = self._domain_heartbeat_service.read_worker_final_target(domain_name, role_name, worker_name)
        final_index_info = final_target["plan"][HapeCommon.ROLE_BIZ_PLAN_KEY[role_name]]["index_info"]
        for index_name in user_index_info:
            index_path = user_index_info[index_name]["index_path"]
            if index_name in final_index_info:
                final_index_info[index_name]["index_path"] = index_path
        final_target["user_cmd"] = HapeCommon.UPF_COMMAND
        ## if there is a updater before
        self._container_service.worker_execute(final_target["plan"][HapeCommon.PROCESSOR_INFO_KEY], "pkill -f .*part_search_update.py.*")
        Logger.info("upf target {}".format(final_target))
        self._domain_heartbeat_service.write_worker_final_target(domain_name, role_name, worker_name, final_target)
        self._domain_heartbeat_service.distribute_target(final_target)
        self._domain_heartbeat_service.remove_worker_user_target(domain_name, role_name, worker_name)
        Logger.info("user target is consumed")
        return True
