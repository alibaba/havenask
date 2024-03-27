import time
import os

from hape_libs.utils.logger import Logger
from hape_libs.utils.shell import LocalShell

class LocalProcUtil():
    
    @staticmethod
    def check_process_pid(command, grep_text=None):
        if grep_text != None:
            check_command = "ps -ef | grep {} | grep {} | grep -v grep".format(command, grep_text)
        else:
            check_command = "ps -ef | grep {} | grep -v grep".format(command)
            
        out = LocalShell.execute_command(check_command)
        for line in out.strip().split("\n"):
            splits = line.split()
            if len(splits) > 1 and splits[1].isdigit():
                Logger.info("Found process, pid {}".format(splits[1]))
                return splits[1]
            
        return None
    
    
    @staticmethod 
    def stop_process(command, grep_text=None):
        Logger.info("Begin to stop processor, command: {}".format(command))
        pid = LocalProcUtil.check_process_pid(command, grep_text)
        if pid != None:
            Logger.info("stop process {}".format(pid))
            command = "kill -9 {}".format(pid)
            LocalShell.execute_command(command)
            return 
        Logger.info("Succeed to stop processor, command: {}".format(command))
        
    @staticmethod
    def start_process(workdir, envs, args, process_command):
        base_name = os.path.basename(workdir)
        Logger.info("Prepare processor resource")
        ## create and check workdir
        LocalShell.execute_command("mkdir -p {}".format(workdir))
        LocalShell.execute_command("ls {} | grep {}".format(os.path.dirname(workdir), base_name), grep_text=base_name)
        
        Logger.info("Begin to start process, command: {}".format(process_command))
        pid = LocalProcUtil.check_process_pid(process_command)
        if pid != None:
            Logger.info("Restart process, stop it first")
            LocalProcUtil.stop_process(process_command)
        
        full_process_command = ""
        if len(envs)!=0:
            full_process_command += " ".join(envs) + " "
        full_process_command += process_command
        if len(args)!=0:
            full_process_command += " " + " ".join(args)
        
        full_process_command += "> stdout.log 2>&1"
            
        command = "mkdir {}".format(workdir)
        LocalShell.execute_command(command)
        command = "cd {} && echo \"{}\" > process_starter.sh".format(workdir, full_process_command + " &")
        LocalShell.execute_command(command)
        command = "cd {} && sh process_starter.sh &".format(workdir, full_process_command)
        LocalShell.execute_command(command, daemonize=True)
        count = 10
        Logger.info("Wait util process is started")
        while count:
            if LocalProcUtil.check_process_pid(process_command) == None:
                Logger.warning("Processor not found, wait {} times".format(count))
                count -= 1
                time.sleep(5)
            else:
                Logger.info("Succeed to start process {}".format(process_command))
                return True
        Logger.error("Failed to start process {}".format(process_command))
        return False
    
    def execute_command(workdir, envs, args, grep_text=None):
        full_process_command = ""
        if len(envs)!=0:
            full_process_command += " ".join(envs) + " "
        full_process_command += command
        if len(args)!=0:
            full_process_command += " " + " ".join(args)
        
        command = "cd {} && {}".format(workdir, full_process_command)
        return LocalShell.execute_command(command, grep_text=grep_text)
