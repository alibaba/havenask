import os
import time
from hape_libs.testlib.zookeeper_wrapper import ZookeeperWrapper
from hape_libs.testlib.port_util import PortUtil

class LocalZkServer(object):
    def __init__(self, install_root, work_dir = None):
        self.install_root = install_root
        self.work_dir = work_dir
        self.zk_wrapper = None
        self.start_succ = False

    def start(self, retry_times = 3):
        zk_install = os.path.join(self.install_root, 'usr/local/share/zookeeper')
        zk_root = None
        while retry_times > 0:
            port = PortUtil.get_unused_port()
            tmp_dir = None
            if self.work_dir:
                tmp_dir = os.path.join(self.work_dir, 'zookeeper_'+str(port))
            self.zk_wrapper = ZookeeperWrapper(zk_install, port, tmp_dir)
            zk_root = self.zk_wrapper.getAddress()
            code = self.zk_wrapper.start()
            if 0 == code:
                self.start_succ = True
                break
            else:
                time.sleep(3)
                retry_times -= 1
        if not self.start_succ:
            return None
        return zk_root

    def stop(self, retry_times = 3):
        if not self.zk_wrapper or not self.start_succ:
            return
        while retry_times > 0:
            code = self.zk_wrapper.stopNotDelData()
            if 0 == code:
                break;
            else:
                time.sleep(3)
                retry_times -= 1
