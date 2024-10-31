import traceback
from .logger import Logger
from kazoo.client import KazooClient

from .shell import LocalShell
import os
import shutil

## user may not have fs_util in hape environment
    
class FsClientBase(object):
    def __init__(self, root_address, binary_path = None):
        splits = self.suffix(root_address).split("/")
        self._address = splits[0]
        self._root_path = "/".join(splits[1:])
        Logger.debug("fs client address {}".format(self._address))
        Logger.debug("fs client root path {}".format(self._root_path))
    
    def suffix(self, address):
        delim = "://"
        index = address.find(delim)
        return address[index+len(delim):]
    
    def exists(self, path):
        raise NotImplementedError
    
    def get(self, path):
        raise NotImplementedError
 
    def mkdir(self, path):
        raise NotImplementedError
    
    def rm(self, path):
        raise NotImplementedError
    
    def write(self, content, path):
        raise NotImplementedError
    
    def complete_path(self, path):
        return "/" + os.path.join(self._root_path, path)
    
    def put(self, src, dest):
        raise NotImplementedError
        

class HdfsClient(FsClientBase):
    def __init__(self, root_address, extend_attrs = {}):
        self.root_address = root_address
        super(HdfsClient, self).__init__(root_address)
        hadoop_home = extend_attrs["hadoop_home"]
        binary_path = extend_attrs["binary_path"]
        self.fs_bin = os.path.join(binary_path, "usr/local/bin/fs_util")
        self.cmd = "HADOOP_HOME={} {}".format(hadoop_home, self.fs_bin)
        
    def execute_wrap_fs_cmd(self, cmd_str):
        cmd = self.cmd + " " + cmd_str
        out, fail = LocalShell.execute_command(cmd, grep_text="fail")
        if fail:
            raise RuntimeError("Failed to execute fs_util command: {}, detail:{}".format(cmd, out))
        return out
        
    
    def rm(self, path):
        Logger.debug("start to delete hdfs path {}".format(path))
        self.execute_wrap_fs_cmd("rm {}".format(path))
        
    def get(self, path):
        Logger.debug("get content of hdfs path {}".format(path))
        result = self.execute_wrap_fs_cmd("cat {}".format(path))
        lines = result.strip().split("\n")
        return "\n".join(lines)
    
    def mkdir(self, path):
        Logger.debug("mkdir hdfs path {}".format(path))
        self.execute_wrap_fs_cmd("mkdir -p {}".format(path))
        
    def exists(self, path):
        try:
            self.execute_wrap_fs_cmd("ls {}".format(path))
            return True
        except:
            return False        
            
    def put(self, src, dest):
        self.execute_wrap_fs_cmd("cp -r {} {}".format(src, dest))
        
    def complete_path(self, path):
        return os.path.join(self.root_address, path)


class LocalClient(FsClientBase):
    def __init__(self, root_address, extend_attrs = {}):
        super(LocalClient, self).__init__(root_address)
        
    def complete_path(self, path):
        return "/" + os.path.join(self._address, self._root_path, path)
        
    def rm(self, path):
        LocalShell.execute_command("rm -rf {}".format(os.path.join(self._address, path)))
        
    def get(self, path):
        with open(path, 'r') as f:
            return f.read()
        
    def mkdir(self, path):
        if not os.path.exists(path):
            os.makedirs(path)
        
    def write(self, content, path):
        if not os.path.exists(os.path.dirname(path)):
            os.makedirs(os.path.dirname(path))
        with open(path, 'w') as f:
            f.write(content)
        return True
    
    def exists(self, path):
        return os.path.exists(path)
    
    def put(self, src, dest):
        if os.path.isfile(src):
            dest_dir = os.path.dirname(dest)
            if not os.path.exists(dest_dir):
                os.makedirs(dest_dir)
            shutil.copyfile(src, dest)
        else:
            shutil.copytree(src, dest)
        
            
class ZfsClient(FsClientBase):
    def __init__(self, root_address, extend_attrs = {}):
        super(ZfsClient, self).__init__(root_address)
        self._client = KazooClient(hosts=self._address)
        self._client.start()
    
    def exists(self, zfs_path):
        return self._client.exists(zfs_path)
    
    def get(self, zfs_path):
        Logger.debug("get zk path {}".format(zfs_path))
        data, stat = self._client.get(zfs_path)
        return data.decode()
 
    def mkdir(self, zfs_path):
        Logger.debug("create zk path {}".format(zfs_path))
        if self.exists(zfs_path):
            Logger.warning("{} already exists, no need to create".format(zfs_path))
            return 
        
        self._client.ensure_path(zfs_path)
    
    def rm(self, zfs_path):
        Logger.debug("remove zk path {}".format(zfs_path))
        if not self.exists(zfs_path):
            Logger.warning("{} not exists, no need to remove".format(zfs_path))
            return 
        try:
            self._client.delete(zfs_path, recursive=True)
        except:
            Logger.debug(traceback.format_exc())
    
    def write(self, content, zfs_path):
        Logger.debug("write zk path {} with content {}".format(zfs_path, content))
        path = zfs_path
        self._client.ensure_path("/".join(path.split("/")[:-1]))
        if not self.exists(zfs_path):
            self._client.create(path, content.encode('utf-8'))
        else:
            self._client.set(path, content.encode('utf-8'))
            
            
            
class FsWrapper:
    FS_CLS = {
        "zfs": ZfsClient,
        "hdfs": HdfsClient,
        "LOCAL": LocalClient
    }
    def __init__(self, root_address, type = None, extend_attrs = {}):
        self._client = None #type: FsClientBase
        if root_address.startswith("/"):
            root_address = "LOCAL:/" + root_address
            
        if type == None:
            for proto in FsWrapper.FS_CLS.keys():
                prefix = proto + "://"
                if root_address.startswith(prefix):
                    Logger.debug("Open {} fs client {}".format(proto, root_address))
                    self._client = FsWrapper.FS_CLS[proto](root_address, extend_attrs = extend_attrs)
        else:
            Logger.debug("Open {} fs client {}".format(type, root_address))
            self._client = FsWrapper.FS_CLS[type](root_address, extend_attrs = extend_attrs)
            
        if self._client == None:
            raise ValueError("Root path [{}] should starts with one of the prefixes [{}]".format(
                root_address, list(FsWrapper.FS_CLS.keys())))
        self.root_address = root_address

    def rm(self, path):
        self._client.rm(self._client.complete_path(path))
    
    def mkdir(self, path):
        self._client.mkdir(self._client.complete_path(path))
    
    
    def write(self, content, path):
        return self._client.write(content,self._client.complete_path(path))
    
    def exists(self, path):
        return self._client.exists(self._client.complete_path(path))
        
    def get(self, path):
        return self._client.get(self._client.complete_path(path))
        
    def put(self, src, dest, filename="__tmp__"):
        # dest = os.path.join(self._client.complete_path(dest), filename)
        Logger.info("upload local files from {} to {}".format(src, dest))
        
        ## can't remove, some processor maybe using it
        if self.exists(dest):
            Logger.warning("{} already exists".format(dest))
            return 
        return self._client.put(src, dest)
    
    def complete_path(self, path):
        return self._client.complete_path(path)