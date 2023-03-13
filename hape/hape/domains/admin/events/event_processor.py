from hape.domains.target import *
from .handlers.start_handler import StartHandler
from .handlers.remove_handler import RemoveHandler
from .handlers.auto_index_load_handler import AutoIndexLoadHandler
from .handlers.qrs_subscribe_handler import QrsSubscribeHandler
from .handlers.upc_handler import UpcHandler
from .handlers.dp_handler import DpHandler
from hape.utils.logger import Logger
import traceback

class EventProcessor:
    def __init__(self, global_conf):
        self._global_conf = global_conf
        self._domain_hb_service = DomainHeartbeatService(global_conf)
        self._handlers = [
        ]
    
    def process(self, domain_name, role_name, worker_name): 
        Logger.info("start to process event for {}".format(worker_name))
        for handler in self._handlers:
            try:
                user_target = self._domain_hb_service.read_worker_user_target(domain_name, role_name, worker_name)
                final_target = self._domain_hb_service.read_worker_final_target(domain_name, role_name, worker_name)
                # Logger.info(handler.name())
                    
                if user_target == None and  final_target == None:
                    Logger.info("user target and final target is None, skip event processor")
                    continue
                
                
                handler_name, command = handler.name(), handler.watch_command()
                Logger.info("try to run handler[{}]".format(handler_name))
                if command != None:
                    if user_target == None or user_target["user_cmd"] != command:
                        if user_target!=None:
                            Logger.info("watch command {} not match user_cmd {}".format(command, user_target["user_cmd"]))
                        Logger.info("handler [{}] not match, skipped".format(handler_name))
                        continue
                    else:
                        Logger.info("handler [{}] matched, need to process".format(handler_name))
                else:
                    Logger.info("user cmd is None")
                if handler.handle(domain_name, role_name, worker_name, user_target) == False:
                    Logger.info("handler [{}] processed failed or not not meet condition, continue".format(handler_name))
                    continue
                Logger.info("handler [{}] processed successfully".format(handler_name))
                self._domain_hb_service.remove_worker_user_target(domain_name, role_name, worker_name)
                Logger.info("user target for handler [{}] is consumed".format(handler_name))
            except:
                error_info = "handler {} failed".format(handler.name())
                Logger.error(error_info)
                Logger.error(traceback.format_exc())
        Logger.info("end to process event for {}".format(worker_name))