# -*- coding: utf-8 -*-
from hape.utils.logger import Logger
from hape.utils.shell import Shell
from hape.common.dfs.dfs import *
import os
import re
import getpass
import time


class ContainerSerivceBase():
    def __init__(self, config):
        self._config = config
        self._user = getpass.getuser()
        
    ## role字段是由于不同role对于机器的需求不同
    def start_worker(self, host_init, processor_target):
        raise NotImplementedError
    
    # def check_worker(self, worker_name):
    #     raise NotImplementedError
    
    def remove_worker(self, worker_name):
        raise NotImplementedError
    
class ContainerServiceFactory():
    @staticmethod
    def create(config): #type:(dict)-> ContainerSerivceBase
        container_service_config = config["container-service"]
        type = container_service_config["type"]
        if type == "docker":
            return RemoteDockerContainerService(config)
        else:
            raise ValueError
    
class RemoteDockerContainerService(ContainerSerivceBase, object):
    def __init__(self, config):
        super(RemoteDockerContainerService, self).__init__(config)
        self._dfs_client = DfsClientFactory.create(config)
        
    # def check_worker(self, processor_target):
    #     shell = Shell(processor_target["ipaddr"])
    #     response = shell.execute_command("docker exec -t {} bash -c \"ps -aux | grep '{}'\" | grep -v grep".format(processor_target["worker_name"], processor_target["command"]))
    #     response = response.strip()
    #     if response.find("No such container") !=-1:
    #         return False
    #     else:
    #         return len(response)!=0
        
        
    def start_worker(self, host_init, processor_target):
        # if self.check_worker(processor_target):
        #     return
        
        shell = Shell(processor_target["ipaddr"])
        worker_name = processor_target["worker_name"]
        Logger.info("start worker {}".format(worker_name))
        workdir = processor_target["workdir"]
        image = processor_target["image"]
        Logger.info("create workdir {}".format(workdir))
        shell.makedir(workdir)
        
                 
        Logger.info("create container")   
        mounts_arg = ""
        mount_dirs = {}
        for key in host_init["init-mounts"]:
            if not os.path.exists(key):
                shell = Shell()
                shell.makedir(key)
            mount_dirs[key] = host_init["init-mounts"][key]
            
        for source in mount_dirs:
            dest = mount_dirs[source]
            mounts_arg += " -v {}:{}".format(source, dest)
            
        envs_arg = ""
        for key in processor_target["envs"]:
            value = processor_target["envs"][key]
            envs_arg += " -e {}={}".format(key, value)
            
        Logger.info("execute init command")  
        shell.execute_command("docker run {} {} -dt --network=host --privileged --name {} {} /usr/sbin/init".format(mounts_arg, envs_arg, worker_name, image))
        init_commands = [
            "useradd -l -u 12345678 -G root -md /home/{} -s /bin/bash {}".format(self._user, self._user),
            "echo -e \\\"\\n{} ALL=(ALL) NOPASSWD:ALL\\n\\\" >> /etc/sudoers".format(self._user),
        ]
        init_commands.extend(host_init["init-commands"])
        for command in init_commands:
            shell.execute_command("docker exec -t {} bash -c \"cd {}; {}\"".format(worker_name, workdir, command))
        
        Logger.info("grant priviledges") 
        shell.execute_command("docker cp ~/.ssh {}:/home/{}".format(worker_name, self._user))
        shell.execute_command("docker exec -t {} bash -c \"chmod 777 -R {}\"".format(worker_name, shell.get_home_path()))
        shell.execute_command("docker exec -t {} bash -c \"chmod 777 -R /home/{}/.ssh\"".format(worker_name, self._user))
        
        Logger.info("host download init packages {}".format(host_init["init-packages"]))
        self.install_package(processor_target, host_init["init-packages"])
        shell.execute_command("docker exec -t {} bash -c \"chmod 777 -R {}/package\"".format(worker_name, workdir))

        command_args = ""
        for elem in processor_target["args"]:
            command_args += " {} {}".format(elem["key"], elem["value"])
        
        Logger.info("start daemon")
        shell.execute_command("docker exec -dt  --user {} {} bash -c \"cd {}; {} {}\"".format(self._user, worker_name, workdir, processor_target["daemon"], command_args))
    
    def install_package(self, processor_target, packages):
        shell = Shell(processor_target["ipaddr"])
        package_dir = os.path.join(processor_target["workdir"], "package")
        shell.execute_command("cd {}; mkdir -p package".format(processor_target["workdir"]))
        Logger.info("install packages {} for worker".format(packages))
        for package in packages:
            Logger.debug("install package [{}] for worker [{}]".format(package, processor_target["worker_name"]))
            self._dfs_client.remote_get(processor_target["ipaddr"], package, package_dir)
    
    def remove_worker(self, processor_target):
        shell = Shell(processor_target["ipaddr"])
        worker_name = processor_target["worker_name"]
        Logger.info("remove worker {}".format(worker_name))
        shell.execute_command("docker rm -f {}".format(worker_name))
        shell.execute_command("cd {}; rm -rf heartbeats".format(processor_target["workdir"]))
    
    def stop_role(self, worker_processor_target_list):
        for worker_processor_target in worker_processor_target_list:
            self._stop_container(worker_processor_target["ip_address"], worker_processor_target["container_name"])