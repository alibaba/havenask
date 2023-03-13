# -*- coding: utf-8 -*-

import os
import socket
import argparse

from hape.utils.dict_file_util import DictFileUtil
from hape.utils.logger import Logger
from hape.utils.shell import Shell
from hape.commands.cmd_base import CommandBase
from hape.common.constants.hape_common import HapeCommon
from hape.domains.target import *
from hape.domains.admin.daemon.daemon_manager import AdminDaemonManager


hape_tool_path = os.path.abspath(os.path.join(os.path.abspath(__file__), "../../../"))


'''
check abnorma in havenask domain 
'''
class CheckCommand(CommandBase, object):
    
    def check(self, args):
        Logger.init_stdout_logger(True)
        parser = argparse.ArgumentParser()
        required_group = parser.add_argument_group('required arguments')
        required_group.add_argument('--config', type=str, dest='config', required=True, help="hape config path of havenask domain")
        self.args = parser.parse_args(args)
        self.config = self.args.config
        self._global_config =  DictFileUtil.read(os.path.join(self.config, "global.conf"))
        self.domain_name = self._strip_domain_name(self.config)
        self._check(self.domain_name)
    
    def _check(self, domain_name):
        Logger.info("start to check abnormal in domain")
        
        Logger.info("check daemon in domain")
        daemon_manager = AdminDaemonManager(self.config)
        daemon_manager.keep_daemon(domain_name)
        if len(daemon_manager.find_daemon(domain_name)) != 1:
            raise RuntimeError("number of domain daemon for [{}] not equals to 1".format(domain_name))
        
        Logger.info("check biz config and role plan")
        self._check_basic_config()
        
        Logger.info("check host machines in domain")
        all_hostips = []
        for role in HapeCommon.ROLES:
            plan = DictFileUtil.read(os.path.join(self.config, "roles", role+"_plan.json"))
            image = plan[HapeCommon.PROCESSOR_INFO_KEY]["image"]
            hostips = plan[HapeCommon.HOSTIPS_KEY]
            if role == "searcher":
                tmp = set([])
                for hostip_row in hostips:
                    for ip in hostip_row:
                        tmp.add(ip)
                hostips = list(tmp)
            all_hostips.extend(hostips)
            
        for ip in set(all_hostips):
            Logger.info("check host:{}".format(ip))
            self._check_ssh(ip)
            self._check_linux_command_installed(ip)
            self._check_image(ip, image)
            self._check_docker_priviledges(ip, image)
            self._check_hdfs(ip)
            
    def _check_linux_command_installed(self, ip):
        shell = Shell(ip)
        response = shell.execute_command("rsync")
        if response.find("Usage") == -1:
            raise RuntimeError("must install rsync command in host:[{}]".format(ip))
            
    def _check_basic_config(self):
        searcher_plan = DictFileUtil.read(os.path.join(self.config, "roles/searcher_plan.json"))
        partition_count = searcher_plan[HapeCommon.ROLE_BIZ_PLAN_KEY["searcher"]]["partition_count"]
        replica_count = searcher_plan[HapeCommon.ROLE_BIZ_PLAN_KEY["searcher"]]["replica_count"]
        hostips = searcher_plan["host_ips"]
        if len(hostips) != replica_count:
            raise RuntimeError("searcher_plan.json: host_ips not match replica and partition count")
        for elem in hostips:
            if type(elem) != list or len(elem) != partition_count:
                raise RuntimeError("searcher_plan.json: host_ips not match replica and partition count")
        
        qrs_plan = DictFileUtil.read(os.path.join(self.config, "roles/qrs_plan.json"))
        replica_count = qrs_plan[HapeCommon.ROLE_BIZ_PLAN_KEY["qrs"]]["replica_count"]
        hostips = qrs_plan["host_ips"]
        if len(hostips) != replica_count:
            raise RuntimeError("qrs_plan.json: host_ips not match replica count")
            
        bs_plan = DictFileUtil.read(os.path.join(self.config, "roles/bs_plan.json"))
        max_part_count = -1
        for index_name in bs_plan[HapeCommon.ROLE_BIZ_PLAN_KEY["bs"]]["index_info"]:
            single_index_info = bs_plan[HapeCommon.ROLE_BIZ_PLAN_KEY["bs"]]["index_info"][index_name]
            partition_count = single_index_info["partition_count"]
            max_part_count = max(partition_count, max_part_count)
            
        hostips = bs_plan["host_ips"]
        if len(hostips) != max_part_count:
            raise RuntimeError("bs_plan.json: host_ips not match partition count")
    
    
    def _check_ssh(self, ip):
        current_ip = socket.gethostbyname(socket.gethostname())
        shell = Shell()
        code, out, error = shell.execute_command("ssh -t -o StrictHostKeyChecking=no -o PasswordAuthentication=no {} 'ls'".format(ip), output_all=True)
        if code != 0:
            raise RuntimeError("ssh connect (StrictHostKeyChecking=no, PasswordAuthentication=no) from {} to {} failed".format(current_ip, ip))
        
        shell = Shell(ip)
        code, out, error = shell.execute_command("ssh -t -o StrictHostKeyChecking=no -o PasswordAuthentication=no {} 'ls'".format(current_ip), output_all=True)
        if code != 0:
            raise RuntimeError("ssh connect (StrictHostKeyChecking=no, PasswordAuthentication=no) from {} to {} failed".format(ip, current_ip))
    
    
    def _check_image(self, ip, image):
        image_name, image_version = image.split(":")
        code, response, error = Shell(ip).execute_command("docker images | grep {}".format(image_name), output_all=True)
        if code != 0:
            flag = False
            for line in response.strip().split("\n"):
                tmp = line.strip().split()
                if len(tmp)>=2 and tmp[0] == image_name and tmp[1] == image_version:
                    flag = True
                    break
            if not flag:
                Logger.info("host lack docker image, ip:[{}] image:[{}]".format(ip, image))
                Logger.info("run docker pull image command in host[{}], it will cost some time".format(ip))
                Logger.info("or you can exit and pull it yourself")
                command = "docker pull {}".format(image)
                shell = Shell(ip)
                shell.execute_command(command, output_all=True)
            
            
    def _check_docker_ps(self, ip, image):
        shell = Shell(ip)
        check_command = "docker ps | grep havenask_check"
        code, out, error = shell.execute_command(check_command, output_all=True)
        if code == 1:
            return False
        elif code == 0:
            return True
        else:
            raise RuntimeError("docker ps (no sudo) in python subprocess failed")
        
    
    def _check_docker_priviledges(self, ip, image):
        shell = Shell(ip)
        remove_command = "docker rm -f havenask_check"
        create_command = "docker run -d --network=host --name havenask_check {} /sbin/init".format(image)
                    
        if self._check_docker_ps(ip, image):
            code, out, error = shell.execute_command(remove_command, output_all=True)
            if code !=0:
                raise RuntimeError("docker remove (no sudo) container in python subprocess failed")
            else:
                if self._check_docker_ps(ip, image):
                    raise RuntimeError("docker remove (no sudo) container in python subprocess failed")
        
        
        code, out, error = shell.execute_command(create_command, output_all=True)
        if code != 0:
            raise RuntimeError("create container (no sudo) in python subprocess failed")
        
        if not self._check_docker_ps(ip, image):
            raise RuntimeError("create container (no sudo) in python subprocess failed")
        
        code, out, error = shell.execute_command(remove_command, output_all=True)
        if code !=0:
            raise RuntimeError("docker remove (no sudo) container in python subprocess failed")
        else:
            if self._check_docker_ps(ip, image):
                raise RuntimeError("docker remove (no sudo) container in python subprocess failed")
    
    
    def _check_hdfs(self, ip):
        if self._global_config["dfs"]["type"] == "hdfs":
            hdfs_path = self._global_config["dfs"]["hadoop-home"]
            hdfs_java_path = self._global_config["dfs"]["hadoop-java-home"]
            Logger.info("dfs client type is hdfs, need to check")
            Logger.info("hdfs path [{}], jdk path [{}]".format(hdfs_path, hdfs_java_path))
            shell = Shell(ip)
            proc, out, error = shell.execute_command("{}/bin/hadoop fs".format(hdfs_path), output_all=True)
            if out.find("Usage: hadoop fs") == -1:
                raise RuntimeError("call hdfs cli failed")
            
            hadoop_envs_path = os.path.join(hdfs_path, "etc/hadoop/hadoop-env.sh")
            response = shell.execute_command("cat {}".format(hadoop_envs_path))
            for line in response.splitlines():
                line = line.strip()
                if line.startswith("export JAVA_HOME"):
                    hadoop_env_java_home = (line.split("=")[-1]).strip()
                    if hadoop_env_java_home != hdfs_java_path:
                        raise RuntimeError("JAVA_HOME [{}] of hdfs tool is not equals to the value [{}] in global.conf".format(hadoop_env_java_home, hadoop_env_java_home, hdfs_java_path))
            
            proc, out, error = shell.execute_command("{}/bin/hadoop fs".format(hdfs_path), output_all=True)
            
        else:
            Logger.info("dfs client type is not hdfs, needless to check")
            


                    
                        

        