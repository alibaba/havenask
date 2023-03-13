from .logger import Logger
import os
import traceback
import socket
import time
import subprocess


class ShellExecuteError(Exception):
    pass

class ShellFileTransferError(Exception):
    pass
        
class Shell:
    def __init__(self, ip=None, workdir=None):
        self._ip = ip
        self._local_ip = socket.gethostbyname(socket.gethostname())
        self._workdir = workdir
        self._shell_name = "shell [{}-{}]".format(ip if ip!=None else "localhost", int(time.time()))
        
    def execute_command(self, command, is_daemon=False, retry_count=0):
        if is_daemon:
            command = command + " > /dev/null 2>&1 & "

        if self._ip != None:
            command = "ssh -o StrictHostKeyChecking=no {} -t '{}'".format(self._ip, command)
            
        Logger.access_log('{}: run {}'.format(self._shell_name, command))
        
        if is_daemon:
            os.system(command)
            return ""   
        
        proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE) 
        out, error = proc.communicate()
        code = proc.returncode
        Logger.access_log("{}: returncode {} response {} {}".format(self._shell_name, code, out, error))
        if code ==  1:
            message = "{}: ERROR: failed to execute command {}".format(self._shell_name, command)
            Logger.access_log(message)
            # raise ShellExecuteError(message)
        return out
    
    def remote_get_file(self, from_path, to_path):
        Logger.access_log("get files of host:[{}], from [{}] to [{}]".format(self._ip, from_path, to_path))
        remote_from_path = from_path
        self.execute_command("rsync -e \"ssh -o StrictHostKeyChecking=no\" -r {} {}".format(remote_from_path, to_path))
            
        dest_path = os.path.join(to_path, os.path.basename(from_path))
        Logger.access_log("check {} {}".format(dest_path, os.path.exists(dest_path)))
        if not os.path.exists(dest_path):
            message = "{}: ERROR: get file failed, ip [{}] remote path:[{}] path:[{}]".format(self._shell_name, self._ip, remote_from_path, dest_path)
            Logger.access_log(message)
            raise ShellFileTransferError(message)
        
    def remote_put_file(self, from_path, to_path):
        Logger.access_log("put files of host:[{}], from [{}] to [{}]".format(self._ip, from_path, to_path))
        remote_to_path = to_path
        self.execute_command("rsync -e \"ssh -o StrictHostKeyChecking=no\" -r {} {}".format(from_path, remote_to_path))
            
        dest_path = os.path.join(to_path, os.path.basename(from_path))
        if not self.file_exists(dest_path):
            message = "{}: ERROR: put file failed, ip [{}] path:[{}]".format(self._shell_name, self._ip, dest_path)
            Logger.access_log(message)
            raise ShellFileTransferError(message)
            
        
    def file_exists(self, path):
        if path.find(":")==-1:
            response = self.execute_command("test -e {}; echo $?".format(path))
        else:
            ip_addr, path = path.split(":")
            response = self.execute_command("ssh -o StrictHostKeyChecking=no {} -t 'test -e {}; echo $?'".format(ip_addr, path))
        return response.find("0")!=-1
    
    def makedir(self, path):
        home_path = self.get_home_path()
        rel_path = os.path.relpath(path, home_path)
        self.execute_command("cd {}; mkdir -p -m 777 {}".format(home_path, rel_path))
        if not self.file_exists(path):
            message = "{}: ERROR: makedir failed, ip [{}] path [{}]".format(self._shell_name, self._ip, path)
            Logger.access_log(message)
            # raise ShellExecuteError(message)
        

    def get_pids(self, processor_name):
        response = self.execute_command("ps -aux | grep \"{}\" | grep -v grep | awk '{{print $2}}'".format(processor_name))
        pids = []
        for raw_line in response.splitlines():
            line = raw_line.strip()
            if line.isdigit():
                pid = int(line)
                if pid == os.getppid():
                    continue
                pids.append(pid)
        return pids

    def kill_pids(self, pids):
        for pid in pids:
            if pid!= os.getpid():
                self.execute_command("kill -9 {}".format(pid))
                

    def get_home_path(self):
        return self.execute_command("eval echo ~$USER").strip()
   
    def get_listen_port(self, pid):
        cmd = "lsof -i -a -P -n -sTCP:LISTEN -p {}".format(str(pid))
        response = self.execute_command(cmd)
        ports = []
        for line in response.splitlines()[1:]:
            ports.append(line.split(":")[1].split()[0])
        return ports
    