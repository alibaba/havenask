from hape.utils.shell import Shell
from hape.utils.logger import Logger
from hape.domains.target import Target
from hape.domains.admin.events.event_handler import EventHandler
from hape.common.constants.hape_common import HapeCommon
import os

'''
when start with role --all, need to deploy index from bs to searcher
'''
class AutoIndexLoadHandler(EventHandler):
    
    def name(self):
        return "auto-index-load"
    
    def watch_command(self):
        return None
        
    def handle(self, domain_name, role_name, worker_name, user_target):
        if role_name != "searcher":
            Logger.warning("not searcher, skip handler {}".format(self.name()))
            return False
        
        
        final_target = self._domain_heartbeat_service.read_worker_final_target(domain_name, role_name, worker_name)
        if final_target == None:
            Logger.info("searcher unready, skip auto index load event")
            return False
        
        searcher_heartbeat = self._domain_heartbeat_service.read_worker_heartbeat(domain_name, role_name, worker_name)
        Logger.info("searcher heartbeat {}".format(searcher_heartbeat))
        if 'plan' not in searcher_heartbeat:
            Logger.warning("searcher not started, skip auto index load event")
            return False
        
        index_info = {}
        for bs_workername in self._domain_heartbeat_service.get_workerlist(domain_name, "bs"):
            bs_heartbeat = self._domain_heartbeat_service.read_worker_heartbeat(domain_name, "bs", bs_workername)
            Logger.info("bs heartbeat {}".format(bs_heartbeat))
            if bs_heartbeat == None or 'plan' not in bs_heartbeat or 'status' not in bs_heartbeat['plan'] or bs_heartbeat["plan"]['status'] != "finished":
                Logger.warning("bs unready, skip auto index load event")
                return False
            else:
                bs_biz_heartbeat = bs_heartbeat["plan"][HapeCommon.ROLE_BIZ_PLAN_KEY["bs"]]
                if 'index_name' in bs_biz_heartbeat:
                    index_name = bs_biz_heartbeat["index_name"]
                    index_path = bs_biz_heartbeat["index_path"]
                    partition_count = bs_biz_heartbeat["partition_count"]
                    index_info[index_name] = {
                        "index_path":index_path,
                        "partition_count": partition_count
                    }
        
        searcher_biz_heartbeat = searcher_heartbeat["plan"][HapeCommon.ROLE_BIZ_PLAN_KEY[role_name]]
        if "index_info" in searcher_biz_heartbeat and len(searcher_biz_heartbeat['index_info']) == len(index_info):
            Logger.warning("searcher already load index, skip auto index load event")
            return False
        else:
            final_target["plan"][HapeCommon.ROLE_BIZ_PLAN_KEY[role_name]]["index_info"] = index_info
            Logger.info("need to handle auto index load event")
            # final_target["user_cmd"] = self.name()
            # target[HapeCommon.ROLE_BIZ_PLAN_KEY[role_name]]["index_path"] = index_path
                
            self._domain_heartbeat_service.write_worker_final_target(domain_name, role_name, worker_name, final_target)
            self._domain_heartbeat_service.distribute_target(final_target)
            return True
