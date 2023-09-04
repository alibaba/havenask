import os
import getpass
import time

from hape_libs.container.container_service import ContainerServiceBase
from hape_libs.utils.logger import Logger
from hape_libs.utils.shell import SSHShell
    

        
class RemoteDockerContainerService(ContainerServiceBase):
    def check_container(self, container_meta, include_all=False):
        shell = SSHShell(container_meta.ip)
        if include_all:
            check_command = "docker ps --format '{{{{.Names}}}}' --all | grep -e ^{}$".format(container_meta.name)
        else:
            check_command = "docker ps --format '{{{{.Names}}}}' | grep -e ^{}$".format(container_meta.name)
        out = shell.execute_command(check_command)
        if len(out.strip()) != 0:
            Logger.info("Container {} is started in {}".format(container_meta.name, shell.ip))
            return True
        else:
            Logger.info("Container {} is not exists in {}".format(container_meta.name, shell.ip))
            return False
        

    def create_container(self, container_meta, container_spec): #type:(ContainerMeta, ContainerSpec)->bool
        Logger.info("Begin to create container {} in {}".format(container_meta.name, container_meta.ip))
        if self.check_container(container_meta, include_all=True):
            Logger.warning("Container {} already exists in {}, will stop it".format(container_meta.name, container_meta.ip))
            self.stop_container(container_meta, None)
            
        shell = SSHShell(container_meta.ip)
        
        check_command = "ls {}".format(self._process_workdir_root)
        _, notexists = shell.execute_command(check_command, grep_text="No such")
        if notexists:
            command = "mkdir -p  {}".format(self._process_workdir_root)
            shell.execute_command(command)
            _, notexists = shell.execute_command(check_command, grep_text="No such")
            if notexists:
                Logger.error("Failed to create directory {}".format(self._process_workdir_root))
                return False
            
        command = "docker run --workdir {} --volume=\"/etc/group:/home/.group:ro\" --volume=\"/etc/passwd:/home/.passwd:ro\"  --volume=\"/etc/hosts:/etc/hosts:ro\"\
 --ulimit nofile=655350:655350  --ulimit memlock=-1  --cpu-quota={} --cpu-period=100000 --memory={}m -v {}:{}  -d --network=host --privileged --name {} {} /sbin/init".format(
                self._process_workdir_root, int(container_spec.cpu) * 1000, container_spec.mem, self._current_homedir, self._current_homedir, container_meta.name, container_spec.image)

        shell.execute_command(command)
        
        workdir = self._process_workdir_root + "/" + container_meta.name
        
        command = "docker exec -t {} bash -c \"cp /home/.passwd /etc/passwd && cp /home/.group /etc/group\"".format(container_meta.name)
        shell.execute_command(command)
        
        command = "docker exec -t --user {} {} bash -c \"mkdir {}\"".format(self._user, container_meta.name, workdir)
        shell.execute_command(command)
        
        if not self.check_container(container_meta):
            Logger.error("Failed to create container {}".format(container_meta.name))
            return False
        else:
            Logger.info("Succeed to create container {}".format(container_meta.name))
            return True
    
    def check_process_pid(self, container_meta, process_spec):
        shell = SSHShell(container_meta.ip)
        check_command = "docker exec -t {} bash -c \"ps -ef | grep {} | grep -v grep\"".format(container_meta.name, process_spec.command)
        out = shell.execute_command(check_command)
        for line in out.strip().split("\n"):
            splits = line.split()
            if len(splits) > 1 and splits[1].isdigit():
                Logger.info("Found process in ip {}, pid {}".format(container_meta.ip, splits[1]))
                return splits[1]
            
        return None
        
    def stop_process(self, container_meta, process_spec):
        pid = self.check_container(container_meta)
        if pid != None:
            Logger.info("Stop process pid:{} ip:{}".format(pid, container_meta.ip))
            shell = SSHShell(container_meta.ip)
            command = "docker exec -t bash -c \"kill -9 {}\"".format(pid)
            return 
        
    def start_process(self, container_meta, process_spec): #type:(ContainerMeta, ProcessorSpec)->bool
        Logger.info("Begin to start process {} in {}".format(process_spec.command, container_meta.ip))
        pid = self.check_process_pid(container_meta, process_spec)
        if pid != None:
            Logger.info("Restart process, stop it first")
            self.stop_process(container_meta, process_spec)
        
        full_process_command = ""
        if len(process_spec.envs)!=0:
            full_process_command += " ".join(process_spec.envs) + " "
        full_process_command += process_spec.command
        if len(process_spec.args)!=0:
            full_process_command += " " + " ".join(process_spec.args)
        
        full_process_command += " 1> stdout.log 2>stderr.log"
            
        shell = SSHShell(container_meta.ip)
        workdir = self._process_workdir_root + "/" + container_meta.name
        command = "docker exec --user {} -t {} bash -c \"cd {} && echo \\\"{}\\\" > process_starter.sh\"".format(self._user, container_meta.name, workdir, full_process_command)
        shell.execute_command(command)
        
        sshme = "docker exec -it --user {} {} bash".format(self._user, container_meta.name)
        command = "docker exec --user {} -t {} bash -c \"cd {} && echo \\\"{}\\\" > sshme\"".format(self._user, container_meta.name, workdir, sshme)
        shell.execute_command(command)
        
        command = "docker exec --user {}  -dt {} bash -c \"cd {} && sh process_starter.sh\"".format(self._user, container_meta.name, workdir)
        shell.execute_command(command)
        count = 10
        Logger.info("Wait util process is started")
        while count:
            if self.check_process_pid(container_meta, process_spec) == None:
                Logger.warning("Processor not found, wait {} times".format(count))
                count -= 1
                time.sleep(5)
            else:
                Logger.info("Succeed to start process {}".format(process_spec.command))
                return True
        Logger.error("Failed to start process {}".format(process_spec.command))
        return False
    
    def execute_command(self, container_meta, process_spec, grep_text = None):
        full_process_command = ""
        if len(process_spec.envs)!=0:
            full_process_command += " ".join(process_spec.envs) + " "
        full_process_command += process_spec.command
        if len(process_spec.args)!=0:
            full_process_command += " " + " ".join(process_spec.args)
        
        shell = SSHShell(container_meta.ip)
        workdir = self._process_workdir_root + "/" + container_meta.name
        command = "docker exec --user {} -t {} bash -c \"cd {} &&{}\"".format(self._user, container_meta.name, workdir, full_process_command)
        return shell.execute_command(command, grep_text=grep_text)
    
    def stop_container(self, container_meta, processor_cmd):
        Logger.info("Stop container {} in {}".format(container_meta.name, container_meta.ip))
        shell = SSHShell(container_meta.ip)
        shell.execute_command("docker ps --all --format {{{{.Names}}}} | grep {} | xargs docker rm -f".format(container_meta.name))
        
    def restart_container(self, container_meta, container_spec, processor_spec):
        self.stop_container(container_meta, processor_spec)
        succ = self.create_container(container_meta, container_spec)
        if not succ:
            Logger.error("Failed to restart container")
            return False
        
        shell = SSHShell(container_meta.ip)
        workdir = self._process_workdir_root + "/" + container_meta.name
        out = shell.execute_command("cd {} && find . -name \"process_starter.sh\"".format(workdir))
        if out.strip() == 0:
            Logger.error("process_starter.sh is not found, cannot restart container")
            return False
        else:
            workdir = os.path.join(workdir, os.path.dirname(out))
            shell.execute_command("docker exec --user {} -dt {} bash -c \" cd {} && sh process_starter.sh\"".format(self._user, container_meta.name, workdir))
            Logger.info("Succeed to restart container")
            return True
    

    
    
