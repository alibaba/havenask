import time

from hape_libs.container.container_service import *
from hape_libs.utils.logger import Logger
from hape_libs.utils.shell import LocalShell
class LocalContainerService(ContainerServiceBase):

    def create_container(self, container_meta, container_spec):
        Logger.info("Begin to create container {}".format(container_meta.name))
        workdir = self._process_workdir_root + "/" + container_meta.name
        ## create and check workdir
        LocalShell.execute_command("mkdir -p {}".format(workdir))
        LocalShell.execute_command("ls {} | grep {}".format(self._process_workdir_root, container_meta.name), grep_text=container_meta.name)
        return True
    
    def check_process_pid(self, container_meta, process_spec):
        if self._processor_tag != None:
            check_command = "ps -ef | grep {} | grep {} | grep -v grep".format(process_spec.command, self._processor_tag)
        else:
            check_command = "ps -ef | grep {} | grep -v grep".format(process_spec.command)
            
        out = LocalShell.execute_command(check_command)
        for line in out.strip().split("\n"):
            splits = line.split()
            if len(splits) > 1 and splits[1].isdigit():
                Logger.info("Found process in ip {}, pid {}".format(container_meta.ip, splits[1]))
                return splits[1]
            
        return None
    
    def check_container(self, container_meta):
        return True
        
    def stop_process(self, container_meta, process_spec):
        Logger.info("Begin to stop container {}".format(container_meta.name))
        pid = self.check_process_pid(container_meta, process_spec)
        if pid != None:
            Logger.info("stop process {}".format(pid))
            command = "kill -9 {}".format(pid)
            LocalShell.execute_command(command)
            return 
        
    def start_process(self, container_meta, process_spec): #type:(ContainerMeta, ProcessorSpec)->bool
        Logger.info("Begin to start process {}".format(process_spec.command))
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
        
        full_process_command += "> stdout.log 2>&1"
            
        workdir = self._process_workdir_root + "/" + container_meta.name
        command = "mkdir {}".format(workdir)
        LocalShell.execute_command(command)
        command = "cd {} && echo \"{}\" > process_starter.sh".format(workdir, full_process_command + " &")
        LocalShell.execute_command(command)
        command = "cd {} && sh process_starter.sh &".format(workdir, full_process_command)
        LocalShell.execute_command(command, daemonize=True)
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
        
        workdir = self._process_workdir_root + "/" + container_meta.name
        command = "cd {} && {}".format(workdir, full_process_command)
        return LocalShell.execute_command(command, grep_text=grep_text)
    
    def stop_container(self, container_meta, processor_cmd):
        Logger.info("Begin to stop container {}".format(container_meta.name))
        workdir = self._process_workdir_root + "/" + container_meta.name
        if self._processor_tag != None:
            LocalShell.execute_command("ps -ef | grep {} | grep {} | grep -v grep | awk '{{print $2}}' | xargs kill -9".format(processor_cmd, self._processor_tag))
        else:
            LocalShell.execute_command("ps -ef | grep {} | grep -v grep | awk '{{print $2}}' | xargs kill -9".format(processor_cmd))
    
    
