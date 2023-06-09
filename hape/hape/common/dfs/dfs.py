# -*- coding: utf-8 -*-

import os

from hape.utils.logger import Logger
from hape.utils.shell import Shell
from hape.utils.shell import Shell

'''
distributed file system client
1. hdfs
2. by ssh rsync command
'''

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
        elif type == "local":
            return LocalClient(storage_config)
        else:
            raise ValueError
        
        
class DfsClientBase():
    def __init__(self, config):
        self._config = config
        
    def remote_download(self, hostip, server_path, local_path):
        raise NotImplementedError
    
    def get(self, server_path, local_path):
        raise NotImplementedError
    
    def put(self, local_path, server_path):
        raise NotImplementedError
    
    def makedir(self, path, is_recursive):
        raise NotImplementedError
    
    def check(self, path, is_dir):
        raise NotImplementedError
            
class HdfsClient(DfsClientBase, object):
    def __init__(self, config):
        super(HdfsClient, self).__init__(config)
        self._hdfs_bin_path = "package/hadoop/bin/hadoop"
    
    def get(self, server_path, local_path):
        Logger.info("dfs client get files from {} to {}".format(server_path, local_path))
        shell = Shell()
        if not os.path.exists(local_path):
            shell.makedir(local_path)
        shell.execute_command("{} fs -get {} {}".format(self._hdfs_bin_path, server_path, local_path))
        full_local_path = os.path.join(local_path, server_path.split("/")[-1])
        if not os.path.exists(full_local_path):
            raise DfsClientError("failed to get {}".format(server_path))
        else:
            Logger.info("get {} succeed".format(server_path))
        return full_local_path
        
    def put(self, local_path, server_path):
        Logger.info("dfs client put files from {} to {}".format(local_path, server_path))
        shell = Shell()
        shell.execute_command("{} fs -put {} {}".format(self._hdfs_bin_path, local_path, server_path))
        full_server_path = os.path.join(server_path, local_path.split("/")[-1])
        if not self.check(full_server_path, is_dir=True):
            raise DfsClientError("failed to put {}".format(full_server_path))
        Logger.info("dfs client put files succeed")
        return full_server_path
    
    def makedir(self, path, is_recursive):
        if is_recursive:
            shell = Shell()
            shell.execute_command("{} fs -mkdir -p {}".format(self._hdfs_bin_path, path))
    
    def check(self, path, is_dir):
        shell = Shell()
        response = shell.execute_command("{} fs -test {} {}; echo $?".format(self._hdfs_bin_path, "-d" if is_dir else "-f", path))
        return response.find("0")!=-1
        
        
class LocalClient(DfsClientBase, object):
    def __init__(self, config):
        super(LocalClient, self).__init__(config)
    
    ## from file server to host local
    def remote_download(self, hostip, server_path, host_local_path):
        Logger.info("dfs client get remote files from {} to {} in host {}".format(server_path, host_local_path, hostip))
        host_shell = Shell(hostip)
        if not host_shell.file_exists(host_local_path):
            host_shell.makedir(host_local_path)
        host_shell.get_remote_file(server_path, host_local_path)
        full_host_local_path = os.path.join(host_local_path, server_path.split("/")[-1])
        Logger.info("dfs client get remote files succeed")
        return full_host_local_path
    
    ## from file server to host local
    def get(self, server_path, host_local_path):
        Logger.info("dfs client get files from {} to {}".format(server_path, host_local_path))
        local_shell = Shell()
        if not os.path.exists(host_local_path):
            local_shell.makedir(host_local_path)
        local_shell.get_remote_file(server_path, host_local_path)
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
        local_shell.put_remote_file(host_local_path, server_path)
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
        
    