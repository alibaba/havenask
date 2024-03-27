import os

from hape_libs.config import *
from hape_libs.utils.logger import Logger
from hape_libs.utils.fs_wrapper import FsWrapper
from hape_libs.utils.shell import LocalShell
from hape_libs.config import GlobalConfig
from hape_libs.testlib.zookeeper_wrapper import ZookeeperWrapper
import traceback

class InfraManager:
    def __init__(self, global_config): #type:(GlobalConfig) -> None
        self._global_config = global_config
        self._common_config = self._global_config.common
        self._binary_path = self._common_config.binaryPath
        self._domain_zk_root = self._common_config.domainZkRoot
        self._zk_port = (self._domain_zk_root.split(":")[-1]).split("/")[0]
        self._zk_server = ZookeeperWrapper(os.path.join(self._binary_path, "usr/local/share/zookeeper"), self._zk_port)
        
    def keep(self):
        if not self._common_config.isDomainZkSet:
            succ = self.start_zookeeper()
            if not succ:
                return False
        return True
    
    def start_zookeeper(self):
        Logger.info("Try to connect to domainZkRoot {}".format(self._domain_zk_root))
        cmd = "{}/usr/local/bin/fs_util ls {}".format(self._binary_path, self._domain_zk_root)
        out, fail = LocalShell.execute_command(cmd, grep_text="loss")
        if not fail:
            Logger.info("DomainZkRoot {} can be visited".format(self._domain_zk_root))
            return True
        
        Logger.info("DomainZkRoot cannot be visited, will try to start test zookeeper")
        Logger.info("If you don't want to use test zookeeper or hape starts test zookeeper timeout, please start zookeeper by hand and fill domainZkRoot in global.conf")
        try:
            Logger.info("Begin to start test zookeeper")
            self._zk_server.start()
            fs_wrapper = FsWrapper(self._domain_zk_root)
            fs_wrapper.mkdir(self._domain_zk_root)
            if fs_wrapper.exists(""):
                Logger.info("Succeed to start test zookeeper")
                return True
        except:
            Logger.warning(traceback.format_exc())
        
        Logger.error("Failed to start test zookeeper or create domainZkRoot {}".format(self._domain_zk_root))
        Logger.error("Please start zookeeper by hand and fill domainZkRoot in global.conf")
        return False
        
        
            
        
        