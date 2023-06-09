import os
import sys
import shutil
import socket
import copy
import argparse
hape_path = os.path.abspath(os.path.join(os.path.abspath(__file__), "../../../"))
sys.path.append(hape_path)

from hape.utils.logger import Logger
from hape.utils.template import *
from hape.utils.dict_file_util import DictFileUtil
from hape.common import HapeCommon
from hape.utils.shell import Shell

'''
 render biz and hape config by templates 
 input: user parameter and template in conf/templates
 output: config in conf/generated
'''

class InitCommand():
    
    def init(self, args):
        parser = argparse.ArgumentParser()
        required_group = parser.add_argument_group('required arguments')
        required_group.add_argument('--iplist', type=str, dest='iplist', required=True, help="ip list of machines in domain. example: <ip1>,<ip2>,<ip3>")
        required_group.add_argument('--domain_name', type=str, dest='domain_name', required=True, help="havenask domain name")
        required_group.add_argument('--index', type=str, dest='index_meta', required=True, help="name and partition count of index. example:<index1-name>:<index1-partition-count1>,<index2-name>:<index2-partition-count>")
        
        option_group = parser.add_argument_group('optional arguments')
        option_group.add_argument('--rt_addr', type=str, dest='rt_addr', help="address of realtime message queue")
        option_group.add_argument('--index_rt_topics', type=str, dest='index_rt_topics', help="realtime message topic of index. example:<index1-topic-name>,<index2-topic-name>")
        args = parser.parse_args(args)
        self._init(args.iplist, args.domain_name, args.index_meta, args.rt_addr, args.index_rt_topics)
    
    def _init(self, iplist, domain_name, index_meta, rt_addr, rt_topics):
        Logger.init_stdout_logger(False)
        index_meta_list = index_meta.split(",")
        temp = []
        for index_meta in index_meta_list:
            single_index_meta = index_meta.split(":")
            index_name, partition_count = single_index_meta[0], single_index_meta[1]
            partition_count = int(partition_count)
            temp.append((index_name, partition_count))
        index_meta_list = temp
        max_partition_count = self._check_index_meta(index_meta_list)
        self._render_biz_config(domain_name, index_meta_list, rt_addr != None)
        self._render_hape_config(iplist, domain_name, index_meta_list, max_partition_count, rt_addr, rt_topics.split(",") if rt_topics != None else None)
        
    def _render_hape_config(self, iplist, domain_name, index_meta_list, max_partition_count, rt_addr, rt_topics):
        iplist = self._expand_iplist(iplist.split(","), max_partition_count)
        Logger.info("iplist expand as {}".format(iplist))
        source_config_path = os.path.join(hape_path, "conf/templates", "hape_config")
        target_config_path = os.path.join(hape_path, "conf/generated", domain_name, domain_name+"_hape_config")
        if os.path.exists(target_config_path):
            shutil.rmtree(target_config_path)
        shutil.copytree(source_config_path, target_config_path)
        
        Logger.info("generate hape config in {}".format(target_config_path))
        
        admin_ip = socket.gethostbyname(socket.gethostname())
        render_vars = {
            "admin_ip": admin_ip,
            "hape_repo": hape_path,
            "partition_count": max_partition_count,
            "homedir": os.path.expanduser("~"),
            "render_domain_name": domain_name,
        }
        
        for role in HapeCommon.ROLES:
            plan_filename = role + "_plan.json"
            Logger.info("render plan {}".format(os.path.join(target_config_path, "roles", plan_filename)))
            template_target = DictFileUtil.read(os.path.join(target_config_path, "roles", plan_filename))
            
            biz_plan_key = HapeCommon.ROLE_BIZ_PLAN_KEY[role]
            processor_info = template_target["processor_info"]
            if "packages" in processor_info:
                processor_info['packages'] = JsonTemplateRender.render(processor_info['packages'], render_vars)
            biz_config = template_target[biz_plan_key]
            if 'config_path' in biz_config:
                # Logger.info("init {} config with {}".format(role, render_vars))
                biz_config['config_path'] = JsonTemplateRender.render_field(biz_config['config_path'], render_vars)
            if 'partition_count' in biz_config:
                biz_config['partition_count'] = max_partition_count


            if role == "bs":
                for idx, (index_name, index_partition_count) in enumerate(index_meta_list):
                    Logger.info("render bs hape config, index:{} partition:{}".format(index_name, index_partition_count))
                    index_info_template = biz_config["index_info"]["${index_name}"]
                    index_render_vars = copy.deepcopy(render_vars)
                    index_render_vars["index_partition_count"] = str(index_partition_count)
                    index_render_vars["index_name"] = index_name
                    single_index_info = JsonTemplateRender.render_field(index_info_template, index_render_vars)
                    single_index_info["partition_count"] = int(single_index_info["partition_count"])
                    if rt_addr != None:
                        rt_topic =  rt_topics[idx]
                        single_index_info["realtime_info"] = {
                            "bootstrap.servers": rt_addr,
                            "topic_name": rt_topic
                        }
                    biz_config["index_info"][index_name] = single_index_info
                    # Logger.info(str(single_index_info))
                del biz_config["index_info"]["${index_name}"]

            reshaped_iplist = copy.deepcopy(iplist)
                
            if role == "qrs":
                reshaped_iplist = [reshaped_iplist[0]]
                
            if role == "searcher":
                reshaped_iplist = [reshaped_iplist]
            else:
                pass
            
            Logger.info("role {}, iplist {}".format(role, reshaped_iplist))
            target = {
                "processor_info": processor_info,
                biz_plan_key: biz_config,
                "host_ips": reshaped_iplist
            }
                
            DictFileUtil.write(os.path.join(target_config_path, "roles", plan_filename), target)
                
        global_config_path = os.path.join(target_config_path, "global.conf")
        global_conf = DictFileUtil.read(global_config_path)
        global_conf = JsonTemplateRender.render(global_conf, render_vars)
        # Logger.info("global conf {}".format(global_conf))
        DictFileUtil.write(global_config_path, global_conf)
        
        host_init_config_path = os.path.join(target_config_path, "host", "host_init.json")
        host_init_conf = DictFileUtil.read(host_init_config_path)
        host_init_conf = JsonTemplateRender.render(host_init_conf, render_vars)
        # Logger.info("global conf {}".format(global_conf))
        DictFileUtil.write(host_init_config_path, host_init_conf)
        Logger.info("init biz and hape config in path:[{}]".format(target_config_path))
    
    
    def _render_biz_config(self, domain_name, index_meta_list, enable_rt):
        source_config_path = os.path.join(hape_path, "conf/templates", "biz_config")
        target_config_path = os.path.join(hape_path, "conf/generated", domain_name, domain_name+"_biz_config")
        Logger.info("generate biz config in {}".format(target_config_path))
        if os.path.exists(target_config_path):
            shutil.rmtree(target_config_path)
        shell = Shell()
        for index_meta in index_meta_list:
            index_name, partition_count = index_meta
            shell.makedir(target_config_path)
            shell.execute_command("cp -r {}/* {}".format(source_config_path, target_config_path))
            Logger.info("index {}, copy from {} to {}".format(index_name, source_config_path, target_config_path))
            ConfigDirRender(target_config_path, {"${index}":index_name, "${bs_partition_count}": partition_count}).render()
        shell.execute_command("mv {} {}".format(target_config_path + "/offline_config", target_config_path + "/" + domain_name+"_offline_config"))
        shell.execute_command("mv {} {}".format(target_config_path + "/online_config", target_config_path + "/" + domain_name+"_online_config"))
        zone_path = os.path.join(target_config_path + "/" + domain_name+"_online_config", "bizs/default/0/zones")
        
        for index_meta in index_meta_list[1:]:
            index_name, partition_count = index_meta
            shutil.rmtree(os.path.join(zone_path, index_name))
        index_name, partition_count = index_meta_list[0]
        shutil.move(os.path.join(zone_path, index_name), os.path.join(zone_path, domain_name))
            
        default_biz = DictFileUtil.read(os.path.join(zone_path, domain_name, "default_biz.json"))
        default_biz["turing_options_config"]["dependency_table"] = [index_meta[0] for index_meta in index_meta_list]
        DictFileUtil.write(os.path.join(zone_path, domain_name, "default_biz.json"), default_biz)
        if enable_rt:
            Logger.info("enable rt")
            table_cluster_path = os.path.join(target_config_path + "/" + domain_name+"_online_config", "table/0/clusters")
            for index_name, partition_count in index_meta_list:
                table_cluster_config = DictFileUtil.read(os.path.join(table_cluster_path, "{}_cluster.json".format(index_name)))
                table_cluster_config["realtime"] = True
                DictFileUtil.write(os.path.join(table_cluster_path, "{}_cluster.json".format(index_name)), table_cluster_config)
        else:
            Logger.info("not enable rt")
        return target_config_path
                            
        
    def _expand_iplist(self, iplist, partition_count):
        convert_iplist = []
        offset = 0
        for i in range(partition_count):
            convert_iplist.append(iplist[offset])
            offset = (offset + 1) % len(iplist)
        return convert_iplist
    
    
    def _check_index_meta(self, index_meta_list):
        if len(index_meta_list) == 0:
            raise ValueError("index meta is empty")
        
        max_partition_count = -1
        for index_meta in index_meta_list:
            index_name, partition_count = index_meta[0], index_meta[1]
            partition_count = int(partition_count)
            max_partition_count  = max(max_partition_count, partition_count)
        
        for index_meta in index_meta_list:
            index_name, partition_count = index_meta[0], index_meta[1]
            partition_count = int(partition_count)
            if partition_count != max_partition_count and partition_count != 1:
                raise ValueError("partition count must be 1 or max")
        return max_partition_count
            
        
        