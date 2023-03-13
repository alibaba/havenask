# -*- coding: utf-8 -*-
from hape.utils.logger import Logger
from hape.utils.shell import Shell
from hape.common.dfs.dfs import *
import os
import getpass


'''
create container by target
'''

class ContainerSerivceBase():
    def __init__(self, config):
        self._config = config
        self._user = getpass.getuser()

    def start_worker(self, host_init, processor_target):
        raise NotImplementedError

    def remove_worker(self, processor_target):
        raise NotImplementedError

    def stop_worker(self, processor_target):
        raise NotImplementedError


    def worker_execute(self, processor_target, command):
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
        self._host_dfs_client = LocalClient({})

    def start_worker(self, host_init, processor_target):
        current_homedir = os.path.expanduser("~")

        shell = Shell(processor_target["ipaddr"], enable_localize=True)
        worker_name = processor_target["worker_name"]
        Logger.info("start worker {}".format(worker_name))
        workdir = processor_target["workdir"]
        image = processor_target["image"]
        Logger.info("create workdir {}".format(workdir))
        shell.makedir(workdir)


        Logger.info("start to create container {}".format(worker_name))
        mounts_arg = ""
        mount_dirs = {}
        for key in host_init["init-mounts"]:
            if not os.path.exists(key):
                shell.makedir(key)
            mount_dirs[key] = host_init["init-mounts"][key]


        if self._config["dfs"]["type"] == "hdfs":
            Logger.info("dfs client type is hdfs, need to mount hadoop and java")
            hdfs_path = self._config["dfs"]["hadoop-home"]
            hdfs_java_path = self._config["dfs"]["hadoop-java-home"]
            mount_dirs[hdfs_path] = "{}/package/hadoop".format(workdir)
            mount_dirs[hdfs_java_path] = hdfs_java_path
        else:
            Logger.info("dfs client type is not hdfs, needless to mount hadoop and java")

        for source in mount_dirs:
            dest = mount_dirs[source]
            mounts_arg += " -v {}:{}".format(source, dest)

        envs_arg = ""
        # for key in processor_target["envs"]:
        #     value = processor_target["envs"][key]
        #     envs_arg += " -e {}={}".format(key, value)


        remove_command = "docker rm -f {}".format(worker_name)
        shell.execute_command(remove_command)
        docker_run_command = "docker run --ulimit nofile=655350:655350  --ulimit memlock=-1 {} {} -d --network=host --privileged --name {} {} /sbin/init".format(mounts_arg, envs_arg, worker_name, image)
        Logger.info("create container by command: {}".format(docker_run_command))
        shell.execute_command(docker_run_command)
        init_commands = [
            "useradd -l -u 12345678 -G root -md {} -s /bin/bash {}".format(current_homedir, self._user),
            "echo -e \\\"\\n{} ALL=(ALL) NOPASSWD:ALL\\n\\\" >> /etc/sudoers".format(self._user),
            "echo export TERM=xterm >> {}/.bashrc".format(current_homedir)
        ]
        init_commands.extend(host_init["init-commands"])
        for command in init_commands:
            shell.execute_command("docker exec -t {} bash -c \"cd {}; {}\"".format(worker_name, workdir, command))

        Logger.info("grant priviledges for worker {}".format(worker_name))
        shell.execute_command("docker cp ~/.ssh {}:{}".format(worker_name, current_homedir))
        shell.execute_command("docker exec -t {} bash -c \"chmod 777 -R {}\"".format(worker_name, shell.get_home_path()))
        shell.execute_command("docker exec -t --user {} {} bash -c \"chmod 700 -R {}/.ssh\"".format(self._user, worker_name, current_homedir))

        Logger.info("host download init packages {}".format(host_init["init-packages"]))
        self.install_package(processor_target, host_init["init-packages"])
        shell.execute_command("docker exec -t {} bash -c \"chmod 777 -R {}/package\"".format(worker_name, workdir))

        command_args = ""
        # for elem in processor_target["args"]:
        #     command_args += " {} {}".format(elem["key"], elem["value"])


        worker_daemon_command = "docker exec -dt  --user {} {} bash -c \"cd {}; {} {}\"".format(self._user, worker_name, workdir, processor_target["daemon"], command_args)
        Logger.info("start worker daemon by command: {}".format(worker_daemon_command))
        shell.execute_command(worker_daemon_command)

    def install_package(self, processor_target, packages):
        shell = Shell(processor_target["ipaddr"], enable_localize=True)
        package_dir = os.path.join(processor_target["workdir"], "package")
        shell.execute_command("cd {}; mkdir -p package".format(processor_target["workdir"]))
        Logger.info("installing packages {}".format(packages))
        for package in packages:
            Logger.debug("install package [{}] for worker [{}]".format(package, processor_target["worker_name"]))
            self._host_dfs_client.remote_download(processor_target["ipaddr"], package, package_dir)

    def stop_worker(self, processor_target):
        shell = Shell(processor_target["ipaddr"], enable_localize=True)
        worker_name = processor_target["worker_name"]
        Logger.info("stop worker {}".format(worker_name))
        ## prevent remove worker have no privileges
        shell.execute_command("docker exec -t {} bash -c \"chmod 777 -R {}\"".format(worker_name, processor_target["workdir"]))
        shell.execute_command("docker rm -f {}".format(worker_name))
        shell.execute_command("cd {}; rm -rf heartbeats".format(processor_target["workdir"]))

    def remove_worker(self, processor_target):
        shell = Shell(processor_target["ipaddr"], enable_localize=True)
        worker_name = processor_target["worker_name"]
        Logger.info("remove worker {}".format(worker_name))
        shell.execute_command("docker rm -f {}".format(worker_name))
        shell.execute_command("rm -rf {}".format(processor_target["workdir"]))

    def worker_execute(self, processor_target, command):
        shell = Shell(processor_target["ipaddr"], enable_localize=True)
        worker_name = processor_target["worker_name"]
        shell.execute_command("docker exec -t {} bash -c  \"{}\"".format(worker_name, command))