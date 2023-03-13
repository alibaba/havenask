# -*- coding: utf-8 -*-

import argparse
import json

from hape.utils.logger import Logger
from hape.commands.cmd_base import CommandBase
from hape.domains.target import *
from hape.domains.admin.daemon.daemon_manager import AdminDaemonManager
from hape.common.constants.hape_common import HapeCommon
from hape.domains.admin.events.handlers import *

'''
subcommand in this file will help to get status or target of havenask domain
'''


def _add_if_have(key, src, desc):
    if key in src:
        desc[key] = src[key]
    
    
def _get_bs_summary(heartbeat_plan, final_target_plan):
    biz_key = HapeCommon.ROLE_BIZ_PLAN_KEY["bs"]
    worker_status = {}
    _add_if_have("status", heartbeat_plan, worker_status)
    if biz_key in heartbeat_plan:
        _add_if_have("config_path", heartbeat_plan[biz_key], worker_status)
        _add_if_have("index_path", heartbeat_plan[biz_key], worker_status)
        _add_if_have("index_name", heartbeat_plan[biz_key], worker_status)
    return worker_status


def _get_qrs_summary(heartbeat_plan, final_target_plan):
    biz_key = HapeCommon.ROLE_BIZ_PLAN_KEY["qrs"]
    worker_status = {}
    _add_if_have("status", heartbeat_plan, worker_status)
    if biz_key in heartbeat_plan:
        _add_if_have("config_path", heartbeat_plan[biz_key], worker_status)
        _add_if_have("qrs_http_port", heartbeat_plan[biz_key], worker_status)
    return worker_status


def _get_searcher_summary(heartbeat_plan, final_target_plan):
    biz_key = HapeCommon.ROLE_BIZ_PLAN_KEY["searcher"]
    worker_status = {}
    _add_if_have("status", heartbeat_plan, worker_status)
    if biz_key in heartbeat_plan:
        _add_if_have("config_path", heartbeat_plan[biz_key], worker_status)
        worker_status["index_info"] = {}
        if "index_info" in heartbeat_plan[biz_key]:
            for index_name in heartbeat_plan[biz_key]["index_info"]:
                worker_status["index_info"][index_name] = {}
                _add_if_have("index_path", heartbeat_plan[biz_key]["index_info"][index_name], worker_status["index_info"][index_name])
            
    return worker_status

class DomainInfoCommandBase(CommandBase, object):
    def init_member(self, hape_config_path):
        super(DomainInfoCommandBase, self).init_member(hape_config_path)
        self._hape_config_path = hape_config_path
        self._daemon_manager = AdminDaemonManager(self._hape_config_path)
        self._domain_heartbeat_service = DomainHeartbeatService(self._global_config, supress_warning=True)
        
        
class StatusCommand(DomainInfoCommandBase, object):
    '''
    hape_cmd {gs}
    
    arguments:
        --config=hape_config_path           : required

        --roles=role of a hape domain       : required

    '''
    summary_funcs = {"qrs": _get_qrs_summary, "bs": _get_bs_summary, "searcher": _get_searcher_summary}

        
    def _get_summary(self, domain_name, role, worker_name):
        heartbeat = self._domain_heartbeat_service.read_worker_heartbeat(domain_name, role, worker_name)
        final_target = self._domain_heartbeat_service.read_worker_final_target(domain_name, role, worker_name)
        user_target = self._domain_heartbeat_service.read_worker_user_target(domain_name, role, worker_name)
        worker_status = {"status": "preparing"}
        if final_target != None:
            if final_target['user_cmd'] != HapeCommon.STOP_WORKER_COMMAND:
                ipaddr = final_target["plan"][HapeCommon.PROCESSOR_INFO_KEY]["ipaddr"]
                worker_status["ipaddr"] = ipaddr
            else:
                ipaddr = final_target["plan"][HapeCommon.PROCESSOR_INFO_KEY]["ipaddr"]
                worker_status["ipaddr"] = ipaddr
                worker_status["status"] = 'stopped'
        elif user_target != None:
            ipaddr = user_target["plan"][HapeCommon.PROCESSOR_INFO_KEY]["ipaddr"]
            worker_status["ipaddr"] = ipaddr
        else:
            worker_status["status"] = "unknown"
        if heartbeat!=None:
            _add_if_have(HapeCommon.LAST_TARGET_TIMESTAMP_KEY["heartbeat"], heartbeat, worker_status)
            worker_status.update(StatusCommand.summary_funcs[role](heartbeat["plan"], final_target["plan"] if final_target!=None else None))
        return worker_status
    
    
    def _get_detail(self, domain_name, role, worker_name):
        heartbeat = self._domain_heartbeat_service.read_worker_heartbeat(domain_name, role, worker_name)
        final_target = self._domain_heartbeat_service.read_worker_final_target(domain_name, role, worker_name)
        user_target = self._domain_heartbeat_service.read_worker_user_target(domain_name, role, worker_name)
        worker_status = {"status": "preparing"}
        if final_target != None:
            ipaddr = final_target["plan"][HapeCommon.PROCESSOR_INFO_KEY]["ipaddr"]
        elif user_target !=None:
            ipaddr = user_target["plan"][HapeCommon.PROCESSOR_INFO_KEY]["ipaddr"]
        else:
            return {}
        worker_status["ipaddr"] = ipaddr
        if heartbeat!=None:
            _add_if_have(HapeCommon.LAST_TARGET_TIMESTAMP_KEY["heartbeat"], heartbeat, worker_status)
            if "plan" in heartbeat:
                worker_status.update(heartbeat["plan"])
        return worker_status
        
    def get_status(self, args):
        self._parser = argparse.ArgumentParser()
        self._parser.add_argument('--config', type=str, dest='config', required=True, help="hape config of havenask domain")
        self._parser.add_argument('--role', choices=["all", "bs", "searcher", "qrs"], dest='role', required=True, help="role in domain")
        self._parser.add_argument('--level', choices=["summary", "detail"], dest='level', default="summary", help="level of information. summary is simpler than detail")
        self.args = self._parser.parse_args(args)
        self.config = self.args.config
        self.roles = [self.args.role] if self.args.role != 'all' else ["searcher", "qrs", "bs"]
        self.domain_name = self._strip_domain_name(self.config)
        self.level = self.args.level
        self.init_member(self.config)
        self._get_status(self.domain_name, self.roles, self.level)

    def _get_status(self, domain_name, roles, level):
        self._daemon_manager.keep_daemon(domain_name)
        domain_status = {
        }
        for role in HapeCommon.ROLES:
            if role not in roles:
                continue
            workername_list = self._domain_heartbeat_service.get_workerlist(domain_name, role)
            domain_status[role] = {}
            for worker_name in workername_list:
                if level == "summary":
                    worker_status = self._get_summary(domain_name, role, worker_name)
                else:
                    worker_status = self._get_detail(domain_name, role, worker_name)
                domain_status[role][worker_name] = worker_status
        print(json.dumps(domain_status, indent=4))
    
class TargetCommand(DomainInfoCommandBase, object):
    
    def get_target(self, args):
        self._parser = argparse.ArgumentParser()
        self._parser.add_argument('--config', type=str, dest='config', required=True, help="hape config of havenask domain")
        self._parser.add_argument('--role', choices=["all", "bs", "searcher", "qrs"], dest='role', required=True, help="role in domain")
        self.args = self._parser.parse_args(args)
        self.config = self.args.config
        self.roles = [self.args.role] if self.args.role != 'all' else ["searcher", "qrs", "bs"]
        self.domain_name = self._strip_domain_name(self.config)
        self.init_member(self.config)
        self._get_target(self.domain_name, self.roles)
        
    def _get_target(self, domain_name, roles):
        self._daemon_manager.keep_daemon(domain_name)
        domain_status = {
        }
        for role in HapeCommon.ROLES:
            if role not in roles:
                continue
            workername_list = self._domain_heartbeat_service.get_workerlist(domain_name, role)
            domain_status[role] = {}
            for worker_name in workername_list:
                final_target = self._domain_heartbeat_service.read_worker_final_target(domain_name, role, worker_name)
                user_target = self._domain_heartbeat_service.read_worker_user_target(domain_name, role, worker_name)
                if final_target == None:
                    final_target = user_target
                domain_status[role][worker_name] = final_target if final_target !=None else {}
        print(json.dumps(domain_status, indent=4))
    