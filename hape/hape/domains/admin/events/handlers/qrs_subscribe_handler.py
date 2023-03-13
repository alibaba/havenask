from hape.utils.shell import Shell
from hape.utils.logger import Logger
from hape.domains.target import Target
from hape.domains.admin.events.event_handler import EventHandler
from hape.common.constants.hape_common import HapeCommon
import os
HERE = os.path.split(os.path.realpath(__file__))[0]
import sys
sys.path.append(os.path.abspath(os.path.join(HERE, "../../../../../","hape_depends")))
from deepdiff import DeepDiff


'''
provide seasrcher service info to qrs
'''

class QrsSubscribeHandler(EventHandler):
    NAME = "qrs-subscribe"
    def watch_command(self):
        return None
    
    def name(self):
        return QrsSubscribeHandler.NAME
    
    def handle(self, domain_name, role_name, worker_name, user_target):
        if role_name != "qrs":
            return False
        Logger.info("handle qrs subscribe")

        subscribe_infos = []
        searcher_workername_list = self._domain_heartbeat_service.get_workerlist(domain_name, "searcher")
        for searcher_workername in searcher_workername_list:
            searcher_heartbeat = self._domain_heartbeat_service.read_worker_heartbeat(domain_name, "searcher", searcher_workername)
            Logger.info("searcher heartbeat {}".format(searcher_heartbeat))
            if searcher_heartbeat == None or "plan" not in searcher_heartbeat or 'status' not in  searcher_heartbeat["plan"] or searcher_heartbeat["plan"]["status"] != "running":
                Logger.info("searcher {} unready, skip".format(searcher_workername))
                return False
            else:
                biz_heartbeat = searcher_heartbeat["plan"][HapeCommon.ROLE_BIZ_PLAN_KEY["searcher"]]
                if "subscribe_info" not in biz_heartbeat:
                    Logger.info("searcher unready, skip qrs subscribe")
                    return False
                else:
                    subscribe_info = biz_heartbeat["subscribe_info"]
                    subscribe_infos.extend(subscribe_info.values())
        
        Logger.info("need to handle qrs subscribe")
        Logger.info("searchers subscribe info {}".format(subscribe_infos))
        
        
        final_target = self._domain_heartbeat_service.read_worker_final_target(domain_name, role_name, worker_name)
        if final_target == None:
            Logger.info("qrs unready, skip auto index load event")
            return False
        
        qrs_heartbeat = self._domain_heartbeat_service.read_worker_final_target(domain_name, role_name, worker_name)
        if qrs_heartbeat == None:
            Logger.info("qrs unready, skip qrs subscribe")
            return False
        qrs_biz_heartbeat = qrs_heartbeat["plan"][HapeCommon.ROLE_BIZ_PLAN_KEY[role_name]]
        Logger.info("qrs biz heartbeat {}".format(qrs_biz_heartbeat))
        if "subscribe_infos" in qrs_biz_heartbeat:
            subscribe_infos_diff = DeepDiff(subscribe_infos, qrs_biz_heartbeat["subscribe_infos"], ignore_order=True)
            Logger.info("qrs subscribe info diff {}".format(subscribe_infos_diff))
            if len(subscribe_infos_diff) == 0:
                Logger.info("qrs already subscribe, skip")
                return False
        
        Logger.info("qrs need to update subscribe")
        
        target = final_target["plan"]
        
        
        target[HapeCommon.ROLE_BIZ_PLAN_KEY[role_name]]["subscribe_infos"] = subscribe_infos
            
        self._domain_heartbeat_service.write_worker_final_target(domain_name, role_name, worker_name, final_target)
        self._domain_heartbeat_service.distribute_target(final_target)
        return True
