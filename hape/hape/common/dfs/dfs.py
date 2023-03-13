# -*- coding: utf-8 -*-

import os

from hape.utils.logger import Logger
from hape.utils.shell import Shell
from hape.utils.shell import Shell

class DfsClientError(Exception):
    pass
class DfsClientFactory():
    @staticmethod
    def create(global_conf):
        storage_config = global_conf["dfs"]
        type = storage_config["type"]
        Logger.info("create dfs client. type:[{}]".format(type))
        if type == "hdfs":
            return HdfsClient(storage_config)
        elif type == "ssh":
            return SSHClient(storage_config)
        else:
            raise ValueError
        
        
class DfsClientBase():
    def __init__(self, config):
        self._config = config
        
    def remote_get(self, hostip, server_path, local_path):
        raise NotImplementedError
    
    def remote_put(self, hostip, server_path, local_path):
        raise NotImplementedError
                
    def get(self, server_path, local_path):
        raise NotImplementedError
    
    def put(self, local_path, server_path):
        raise NotImplementedError
    
    def makedir(self, path, is_recursive):
        raise NotImplementedError
    
    def check(self, path, is_dir):
        raise NotImplementedError
            
class HdfsClient(DfsClientBase):
    
    def get(self, server_path, local_path):
        if not os.path.exists(local_path):
            os.makedirs(local_path)
        shell = Shell()
        shell.execute_command("hadoop fs -get {} {}".format(server_path, local_path))
        full_local_path = os.path.join(local_path, server_path.split("/")[-1])
        return full_local_path
        
    def put(self, local_path, server_path):
        shell = Shell()
        shell.execute_command("hadoop fs -put {} {}".format(local_path, server_path))
        full_server_path = os.path.join(server_path, local_path.split("/")[-1])
        return full_server_path
    
    def makedir(self, path, is_recursive):
        if is_recursive:
            shell = Shell()
            shell.execute_command("hadoop fs -mkdir -p {}".format(path))
    
    def check(self, path, is_dir):
        if is_dir:
            shell = Shell()
            proc, response, code = shell.execute_command("hadoop fs -test -d {}; echo $?".format(path))
            return response.find("0")!=-1
        
        
class SSHClient(DfsClientBase, object):
    def __init__(self, config):
        super(SSHClient, self).__init__(config)
    
    ## from file server to host local
    def remote_get(self, hostip, server_path, host_local_path):
        Logger.info("dfs client get remote files from {} to {} in host {}".format(server_path, host_local_path, hostip))
        host_shell = Shell(hostip)
        if not host_shell.file_exists(host_local_path):
            host_shell.makedir(host_local_path)
        host_shell.remote_get_file(server_path, host_local_path)
        full_host_local_path = os.path.join(host_local_path, server_path.split("/")[-1])
        Logger.info("dfs client get remote files succeed")
        return full_host_local_path
    
    ## from file server to host local
    def get(self, server_path, host_local_path):
        Logger.info("dfs client get files from {} to {}".format(server_path, host_local_path))
        local_shell = Shell()
        if not os.path.exists(host_local_path):
            local_shell.makedir(host_local_path)
        local_shell.remote_get_file(server_path, host_local_path)
        full_host_local_path = os.path.join(host_local_path, server_path.split("/")[-1])
        if not os.path.exists(full_host_local_path):
            raise DfsClientError("failed to get {}".format(server_path))
        else:
            Logger.info("get {} succeed".format(server_path))
        Logger.info("dfs client get files succeed")
        return full_host_local_path
        
    def put(self, host_local_path, server_path):
        Logger.info("dfs client put files from {} to {}".format(host_local_path, server_path))
        local_shell = Shell()
        local_shell.remote_put_file(host_local_path, server_path)
        full_server_path = os.path.join(server_path, host_local_path.split("/")[-1])
        Logger.info("dfs client put files succeed")
        return full_server_path
    
    def makedir(self, path, is_recursive):
        address, path = path.split(":")
        server_shell = Shell(address)
        if is_recursive:
            server_shell.makedir(path)
    
    def check(self, path, is_dir):
        address, path = path.split(":")
        server_shell = Shell(address)
        return server_shell.file_exists(path)
        
    