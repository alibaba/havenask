# -*- coding: utf-8 -*-

import os
import json
import copy
import threading
import time
import argparse

from hape.utils.dict_file_util import DictFileUtil
from hape.utils.logger import Logger
from hape.commands.cmd_base import CommandBase
from hape.domains.target import *
from hape.domains.admin.daemon.daemon_manager import AdminDaemonManager
from hape.common.constants.hape_common import HapeCommon
from hape.utils.partition import get_partition_intervals
from hape.utils.template import JsonTemplateRender
from hape.utils.shell import Shell
from hape.domains.admin.events.handlers import *


'''
subcommand in this file will change target of havenask domain 
'''

class PlanCommandBase(CommandBase, object):
    
    def init_member(self, hape_config_path):
        super(PlanCommandBase, self).init_member(hape_config_path)
        self._daemon_manager = AdminDaemonManager(self._hape_config_path)
        self._domain_heartbeat_service = DomainHeartbeatService(self._global_config)
        
    def _get_worker_name(self, domain_name, role_name, repid, part_id, part_interval, index_name = None):
        if index_name != None:
            role_name = "index_{}".format(index_name)
        return "-".join([self._global_config["hape-worker"]["worker-name-prefix"],
                            domain_name, role_name, str(repid), str(part_id),str(part_interval[0])+ "." + str(part_interval[1])])

    def _get_workdir_path(self, worker_name):
        return os.path.join(self._global_config["hape-worker"]["workdir"], worker_name)


class StartCommand(PlanCommandBase, object):

    def start(self, args):
        parser = argparse.ArgumentParser()
        required_group = parser.add_argument_group('required arguments')
        required_group.add_argument('--config', type=str, dest='config', required=True, help="hape config path of havenask domain")
        required_group.add_argument('--role', choices=["all", "bs", "searcher", "qrs"], dest='role', required=True, help="role of workers in domain")
        option_group = parser.add_argument_group('optional arguments')
        option_group.add_argument('--worker', type=str, dest='worker', help="worker name in domain")
        self.args = parser.parse_args(args)
        self.config = self.args.config
        self.roles = [self.args.role] if self.args.role != 'all' else ["searcher", "qrs", "bs"]
        self.domain_name = self._strip_domain_name(self.config)
        self.worker = self.args.worker
        self.init_member(self.config)
        self._start(self.domain_name, self.roles, self.worker)
    
    def build_full_index(self, args):
        parser = argparse.ArgumentParser()
        required_group = parser.add_argument_group('required arguments')
        required_group.add_argument('--config', type=str, dest='config', required=True, help="hape config path of havenask domain")
        required_group.add_argument('--index_name', type=str, dest='index_name', required=True, help="name of index to build")
        self.args = parser.parse_args(args)
        self.config = self.args.config
        self.domain_name = self._strip_domain_name(self.config)
        self.build_index_name = self.args.index_name
        self.init_member(self.config)
        self._start(self.domain_name, ["bs"], None, build_index_name = self.build_index_name)
    
    def _start(self, domain_name, roles, worker, build_index_name=None):
        self._daemon_manager.keep_daemon(domain_name)
        if worker == None:
            Logger.info("start command, domain name [{}] roles [{}]".format(domain_name, roles))
            for role_name in roles:
                plan = DictFileUtil.read(os.path.join(self._hape_config_path, "roles", "{}_plan.json").format(role_name))
                if role_name != "bs":
                    self._gen_start_target_role(domain_name, role_name, plan)
                else:
                    biz_plan_key = HapeCommon.ROLE_BIZ_PLAN_KEY["bs"] 
                    for index_name in plan[HapeCommon.ROLE_BIZ_PLAN_KEY["bs"]]["index_info"]:
                        if build_index_name != None and index_name != build_index_name:
                            continue
                        copy_plan = copy.deepcopy(plan)
                        index_plan = copy_plan[HapeCommon.ROLE_BIZ_PLAN_KEY["bs"]]["index_info"][index_name]
                        index_plan["index_name"] = index_name
                        generation_id = int(time.time())
                        index_plan["generation_id"] = generation_id
                        copy_plan[biz_plan_key] = index_plan
                        self._gen_start_target_role(domain_name, role_name, copy_plan)
        else:
            role = roles[0]
            Logger.info("start command, domain name [{}] role {} worker [{}]".format(domain_name, role, worker))
            final_target = self._domain_heartbeat_service.read_worker_final_target(domain_name, role, worker)
            user_target = final_target
            user_target['type'] = Target.USER_TARGET_TYPE
            user_target['user_cmd'] = HapeCommon.START_WORKER_COMMAND
            self._domain_heartbeat_service.write_worker_user_target(domain_name, role, worker, user_target)

    def _gen_start_target_role(self, domain_name, role_name, plan):
        biz_plan = plan[HapeCommon.ROLE_BIZ_PLAN_KEY[role_name]]
        replica_count = biz_plan.get("replica_count", 1)
        partition_count = biz_plan.get("partition_count", 1)
        biz_plan["replica_count"] = replica_count
        biz_plan["partition_count"] = partition_count
        hostips = plan[HapeCommon.HOSTIPS_KEY]
        if role_name == "qrs":
            hostips = [[ip] for ip in hostips]
        elif role_name == "bs":
            hostips = [hostips]
        Logger.info("replica count:{} partition count:{} hostips:{}".format(replica_count, partition_count, hostips))
        threads = []
        for repid in range(replica_count):
            for partid in range(partition_count):
                rendered_plan = self._gen_worker_target(domain_name, role_name, copy.deepcopy(plan), 
                                                        repid, partid, hostips[repid][partid], index_name = None if role_name!="bs"  else plan[HapeCommon.ROLE_BIZ_PLAN_KEY[role_name]]["index_name"])
                t = threading.Thread(target = self._write_start_target_worker, args = (domain_name, role_name, rendered_plan))
                t.name = rendered_plan[HapeCommon.PROCESSOR_INFO_KEY]["worker_name"]
                t.start()
                threads.append(t)
        for thread in threads:
            thread.join()
                        
                        
    def _add_host_init(self, worker_target):
        host_init_path = os.path.join(self._hape_config_path, "host", "host_init.json")
        host_init_config = DictFileUtil.read(host_init_path)
        host_init_config["init-mounts"] = JsonTemplateRender.render_field(host_init_config["init-mounts"], 
                            {"role_name": worker_target.role_name, "worker_name": worker_target.worker_name, "workdir": worker_target.plan[HapeCommon.PROCESSOR_INFO_KEY]["workdir"]})
        host_init_config["init-packages"] = JsonTemplateRender.render_field(host_init_config["init-packages"],
                            {"role_name": worker_target.role_name, "worker_name": worker_target.worker_name, "workdir": worker_target.plan[HapeCommon.PROCESSOR_INFO_KEY]["workdir"]})
        host_init_config["init-commands"] = JsonTemplateRender.render_field(host_init_config["init-commands"],
                            {"role_name": worker_target.role_name, "worker_name": worker_target.worker_name, "workdir": worker_target.plan[HapeCommon.PROCESSOR_INFO_KEY]["workdir"]})

        worker_target.host_init = host_init_config
    
    def _write_start_target_worker(self, domain_name, role_name, rendered_plan):
        
        worker_target = Target(Target.USER_TARGET_TYPE, rendered_plan, domain_name, role_name, 
                                rendered_plan[HapeCommon.PROCESSOR_INFO_KEY]["worker_name"], HapeCommon.START_WORKER_COMMAND)
        
        self._add_host_init(worker_target)
        Logger.info("submmit target for worker:{} target:{}".format(rendered_plan[HapeCommon.PROCESSOR_INFO_KEY]["worker_name"], json.dumps(worker_target.to_dict(),indent=4))) 

        self._domain_heartbeat_service.write_worker_user_target(domain_name, role_name, worker_target.worker_name, worker_target.to_dict())
        
    def _gen_worker_target(self, domain_name, role_name, roleplan, repid, partid, ipaddr, index_name):
        biz_plan =  roleplan[HapeCommon.ROLE_BIZ_PLAN_KEY[role_name]]
        processor_plan = roleplan[HapeCommon.PROCESSOR_INFO_KEY]
        part_intervals = get_partition_intervals(biz_plan["partition_count"])
        Logger.info("part intervals {} partid {}".format(part_intervals, partid))
        part_interval = part_intervals[partid]
        worker_name = self._get_worker_name(domain_name, role_name, repid, partid, part_interval, index_name)
        workdir = self._get_workdir_path(worker_name)
        processor_target = {
            "ipaddr": ipaddr,
            "worker_name": worker_name,
            "workdir": workdir,
            "image": processor_plan["image"],
            "replica_id": repid,
            "domain_name": domain_name, 
            "role_name": role_name, 
        }
        
        render_vars = {
            "workdir": workdir,
            "domain_name": domain_name,
            "role_name": role_name,
            "worker_name": worker_name
        }
        daemon_cmd = "python package/hape/domains/workers/worker_daemon.py --domain_name ${domain_name} --role_name ${role_name} --worker_name ${worker_name} --global_conf package/global.conf"
        daemon_cmd = JsonTemplateRender.render_content(daemon_cmd, render_vars)
        processor_target["daemon"] = daemon_cmd

        processor_target["envs"] = JsonTemplateRender.render_field(processor_plan["envs"], render_vars)
        processor_target["args"] = JsonTemplateRender.render(processor_plan["args"], render_vars)
        processor_target["packages"] = processor_plan["packages"]
        roleplan[HapeCommon.PROCESSOR_INFO_KEY] = processor_target
        roleplan[HapeCommon.ROLE_BIZ_PLAN_KEY[role_name]]["partition_id"] = partid
        roleplan[HapeCommon.ROLE_BIZ_PLAN_KEY[role_name]]["replica_id"] = repid
        return roleplan

class RemoveCommand(PlanCommandBase, object):
    def remove(self, args):
        parser = argparse.ArgumentParser()
        required_group = parser.add_argument_group('required arguments')
        required_group.add_argument('--config', type=str, dest='config', required=True, help="hape config path of havenask domain")
        required_group.add_argument('--role', choices=["all", "bs", "searcher", "qrs"], dest='role', required=True, help="role in domain")
        self.args = parser.parse_args(args)
        self.config = self.args.config
        self.roles = [self.args.role] if self.args.role != 'all' else ["searcher", "qrs", "bs"]
        self.domain_name = self._strip_domain_name(self.config)
        self.init_member(self.config)
        self._remove(self.domain_name, self.roles)       
            
    def _remove(self, domain_name, roles):
        self._daemon_manager.keep_daemon(domain_name)
        Logger.info("remove plan command, domain name [{}]".format(domain_name))
        for role_name in roles:
            worker_name_list = self._domain_heartbeat_service.get_workerlist(domain_name, role_name)
            for worker_name in worker_name_list:
                final_target = self._domain_heartbeat_service.read_worker_final_target(domain_name, role_name, worker_name)
                if final_target == None:
                    continue
                target = Target(Target.USER_TARGET_TYPE, final_target["plan"], domain_name, role_name, worker_name, HapeCommon.REMOVE_WORKER_COMMAND)
                self._domain_heartbeat_service.write_worker_user_target(domain_name, role_name, worker_name, target.to_dict())
   
   
class StopCommand(PlanCommandBase, object):             
    def stop(self, args):
        parser = argparse.ArgumentParser()
        required_group = parser.add_argument_group('required arguments')
        required_group.add_argument('--config', type=str, dest='config', required=True, help="hape config path of havenask domain")
        required_group.add_argument('--role', choices=["all", "bs", "searcher", "qrs"], dest='role', required=True, help="role in domain")
        self.args = parser.parse_args(args)
        self.config = self.args.config
        self.roles = [self.args.role] if self.args.role != 'all' else ["searcher", "qrs", "bs"]
        self.domain_name = self._strip_domain_name(self.config)
        self.init_member(self.config)
        self._stop(self.domain_name, self.roles) 
                
                
    def _stop(self, domain_name, roles):
        self._daemon_manager.keep_daemon(domain_name)
        Logger.info("stop plan command, domain name [{}]".format(domain_name))
        for role_name in roles:
            worker_name_list = self._domain_heartbeat_service.get_workerlist(domain_name, role_name)
            for worker_name in worker_name_list:
                final_target = self._domain_heartbeat_service.read_worker_final_target(domain_name, role_name, worker_name)
                if final_target == None:
                    continue
                target = Target(Target.USER_TARGET_TYPE, final_target["plan"], domain_name, role_name, worker_name, HapeCommon.STOP_WORKER_COMMAND)
                self._domain_heartbeat_service.write_worker_user_target(domain_name, role_name, worker_name, target.to_dict())
    
    
class UpcCommand(PlanCommandBase, object):
    def upc(self, args):
        parser = argparse.ArgumentParser()
        required_group = parser.add_argument_group('required arguments')
        required_group.add_argument('--config', type=str, dest='config', required=True, help="hape config path of havenask domain")
        required_group.add_argument('--role', choices=["searcher", "qrs"], dest='role', required=True, help="role in domain")
        self.args = parser.parse_args(args)
        self.config = self.args.config
        self.roles = [self.args.role]
        self.domain_name = self._strip_domain_name(self.config)
        self.init_member(self.config)
        self._upc(self.domain_name, self.roles) 
    
    def _upc(self, domain_name, roles):
        self._daemon_manager.keep_daemon(domain_name)
        
        for role_name in HapeCommon.ROLES:
            if role_name not in roles:
                continue
            
            plan = DictFileUtil.read(os.path.join(self._hape_config_path, "roles", role_name + "_plan.json"))
            for worker_name in self._domain_heartbeat_service.get_workerlist(domain_name, role_name):
                user_target = Target(Target.USER_TARGET_TYPE, plan, domain_name, role_name, worker_name, HapeCommon.UPC_COMMAND)
                self._domain_heartbeat_service.write_worker_user_target(domain_name, role_name, worker_name, user_target.to_dict())
      
      
class UpfCommand(PlanCommandBase, object):          
    def upf(self, args):
        parser = argparse.ArgumentParser()
        required_group = parser.add_argument_group('required arguments')
        required_group.add_argument('--config', type=str, dest='config', required=True, help="hape config path of havenask domain")
        required_group.add_argument('--index_name', type=str, dest='index_name', required=True, help = "index name to update")
        required_group.add_argument('--index_path', type=str, dest='index_path', required=True, help = "index path to update")

        self.args = parser.parse_args(args)
        self.config = self.args.config
        self.domain_name = self._strip_domain_name(self.config)
        self.index_name = self.args.index_name
        self.index_path = self.args.index_path
        self.init_member(self.config)
        self._upf(self.domain_name, self.index_name, self.index_path) 
                
    def _upf(self, domain_name, index_name, index_path):
        self._daemon_manager.keep_daemon(domain_name)
        plan = DictFileUtil.read(os.path.join(self._hape_config_path, "roles",  "searcher_plan.json"))
        for worker_name in self._domain_heartbeat_service.get_workerlist(domain_name, "searcher"):
            plan[HapeCommon.ROLE_BIZ_PLAN_KEY["searcher"]]["index_info"] = {
                index_name: {
                    "index_path": index_path
                }
            }
            user_target = Target(Target.USER_TARGET_TYPE, plan, domain_name, "searcher", worker_name, HapeCommon.UPF_COMMAND)
            
            self._domain_heartbeat_service.write_worker_user_target(domain_name, "searcher", worker_name, user_target.to_dict())


class DpCommand(PlanCommandBase, object):
    def dp(self, args):
        parser = argparse.ArgumentParser()
        required_group = parser.add_argument_group('required arguments')
        required_group.add_argument('--config', type=str, dest='config', required=True, help="hape config path of havenask domain")
        required_group.add_argument('--role', choices=["all", "bs", "searcher", "qrs"], dest='role', required=True, help="role in domain")
        self.args = parser.parse_args(args)
        self.config = self.args.config
        self.roles = [self.args.role] if self.args.role != 'all' else ["searcher", "qrs", "bs"]
        self.domain_name = self._strip_domain_name(self.config)
        self.init_member(self.config)
        self._dp(self.domain_name, self.roles) 
    
    def _dp(self, domain_name, roles):
        self._daemon_manager.keep_daemon(domain_name)
        
        for role_name in HapeCommon.ROLES:
            if role_name not in roles:
                continue
            
            plan = DictFileUtil.read(os.path.join(self._hape_config_path, "roles", role_name + "_plan.json"))
            for worker_name in self._domain_heartbeat_service.get_workerlist(domain_name, role_name):
                user_target = Target(Target.USER_TARGET_TYPE, plan, domain_name, role_name, worker_name, HapeCommon.DP_COMMAND)
                self._domain_heartbeat_service.write_worker_user_target(domain_name, role_name, worker_name, user_target.to_dict())
        
        
        
        