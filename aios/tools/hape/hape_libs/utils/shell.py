from .logger import Logger
import subprocess
import json


class SSHShell:
    def __init__(self, ip = None):
        self.ip = ip
            
    def execute_command(self, command, grep_text = None):
        full_command = "ssh -o PasswordAuthentication=no -o StrictHostKeyChecking=no {} -t '{}'".format(self.ip, command)
        message = '[SSH:{}] Execute command: [\n{}\n]'.format(self.ip, full_command)
        Logger.debug(message)
        proc = subprocess.Popen(full_command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE) 
        out, error = proc.communicate()
        try:
            out, error = out.decode(), error.decode()
        except:
            pass
        full_out = out + "\n" + error
        response_message = "[SSH:{}] Command response: {}".format(self.ip, full_out)
        Logger.debug(response_message)
        
        if error.find("Permission denied")!=-1:
            raise RuntimeError("Cannot execute ssh command in ip {}, detail: {}".format(self.ip, full_out))
            
        if grep_text != None:
            succ = (out.find(grep_text) != -1)
            return out, succ
                
        else:
            return out
        
    def makedir(self, abs_path):
        self.execute_command("mkdir -p -m 777 {}".format(abs_path))
        
    def exists(self, abs_path):
        out, exists = self.execute_command("test -e {}; echo $?".format(abs_path), grep_text="0")
        return exists
        
        
        

class LocalShell:
    @staticmethod
    def execute_command(command, daemonize = False, grep_text = None):
        message = 'Execute local shell command: [{}]'.format(command)
        Logger.debug(message)
        proc = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE) 
        if daemonize:
            return True
        
        out, error = proc.communicate()
        try:
            out, error = out.decode(), error.decode()
        except:
            pass
        out, error = out.strip(), error.strip()
        full_out = out + "\n" if len(out) !=0 else '' + error
        response_message = "Local shell command response: [{}]".format(full_out)
        Logger.debug(response_message)
        if grep_text != None:
            succ = (out.find(grep_text) != -1)
            return full_out, succ
                
        else:
            return out
    
    @staticmethod
    def rsync(src_path, dest_path):
        cmd = "rsync -e \"ssh -o PasswordAuthentication=no -o StrictHostKeyChecking=no\" -r {} {}".format(src_path, dest_path)
        LocalShell.execute_command(cmd)