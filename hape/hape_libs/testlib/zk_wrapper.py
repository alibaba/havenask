import logging
from hape_libs.testlib.local_zk_server import *
from hape_libs.testlib.module_base import ModuleBase

class ZkWrapper(ModuleBase, LocalZkServer):
    def __init__(self, path_root=None):
        ModuleBase.__init__(self, path_root)
        LocalZkServer.__init__(self, self.install_root, work_dir = self.install_root)
        self.host = None
        self.pid = None

    def start_zk(self, retry_times = 3):
        self.host = self.start(retry_times)
        if not self.host:
            # logging.error(f'failed to start zk with retry_times:[{str(retry_times)}]')
            return False
        # logging.info(f'local zk host is [{self.host}], rety[{str(retry_times)}]')
        self.get_pid()
        return self.host is not None

    def stop_zk(self, retry_times=1):
        self.stop(retry_times)

    def check_zk(self, action='ls', path=''):
        cmd = '%s/fs_util_bin %s %s' % (self.bin_path, action, path)
        read_zk_cmd = self.ld_library_cmd() + cmd
        logging.info(read_zk_cmd)
        import datetime
        import subprocess

        proc = subprocess.Popen(read_zk_cmd, stdout=subprocess.PIPE, shell=True)
        now = datetime.datetime.now()
        (out, err) = proc.communicate()
        # logging.info(f'check zk path [{path}] action [{action}] res [{str(out)}] now time[{str(now.time())}]')
        return (str(out).find("no node")) == -1

    def __get_port(self, host):
        port = None
        zk_prefix = 'zfs://'
        if host.startswith(zk_prefix):
            host = host[len(zk_prefix):]
            idx = host.find(':')
            port = host[idx+1:]
        return port

    def get_pid(self):
        port = self.__get_port(self.host)
        pids = self._get_pid('zookeeper', extra=port)
        self.pid = pids[0]
