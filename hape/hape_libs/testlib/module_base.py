import os
import logging
import shutil
import subprocess

class ModuleBase(object):
    def __init__(self, path_root=None):
        self.path_root = os.path.realpath(os.path.join(os.path.dirname(__file__), '../')) if path_root is None else path_root
        self.install_root = os.path.join(self.path_root, 'install_root')
        self.local_path = os.path.join(self.install_root, 'usr/local')
        self.bin_path = os.path.join(self.local_path, 'bin')
        self.etc_path = os.path.join(self.local_path, 'etc')
        self.suspend_flag = False

    # temporary solution, should search the files need unlink later
    def remove_symlinks(self, target_dir = None):
        file_list = []
        file_list.append(os.path.join(self.bin_path, 'fs_util'))
        file_list.append(os.path.join(self.bin_path, 'swift'))
        file_list.append(os.path.join(self.etc_path, 'swift_tools/tools_conf.py'))

        for f in file_list:
            ModuleBase.remove_symlink(f)

        if target_dir is not None:
            if os.path.exists(target_dir):
                shutil.rmtree(target_dir)
            shutil.copytree(self.install_root, target_dir, symlinks=True)

    def ld_library_cmd(self):
        ld_cmd = 'LD_LIBRARY_PATH=%s/lib64:%s/lib:$LD_LIBRARY_PATH PATH=%s:$PATH ' % (self.local_path, self.local_path, self.bin_path)
        return ld_cmd

    @staticmethod
    def remove_symlink(file_path):
        if os.path.islink(file_path):
            try:
                src = os.readlink(file_path)
                os.unlink(file_path)
                shutil.copy(src, file_path)
            except FileNotFoundError:
                logging.info("file[{}] not exist, maybe unlink already.".format(file_path))
    '''
    @staticmethod
    def path_root():
        return os.path.realpath(os.path.join(os.path.dirname(__file__), '../'))

    @staticmethod
    def install_root():
        return os.path.join(ModuleBase.path_root(), 'install_root')

    @staticmethod
    def bin_path():
        return os.path.join(ModuleBase.install_root(), 'usr/local/bin')
    '''
    def _get_pid(self, module_name ,extra=None):
        cmd = 'ps uxww | grep %s | grep -v grep' %(module_name)
        if extra:
            if type(extra) is str:
                cmd = cmd + ' | grep %s' % extra
            if type(extra) is list:
                for s in extra:
                    cmd = cmd + ' | grep %s' % s
        p = subprocess.Popen(cmd, shell=True, close_fds=False, stdout=subprocess.PIPE)
        out, err = p.communicate()

        pids = []
        for line in out.splitlines():
            parts = line.split()
            pids.append(int(parts[1]))

        return pids
    
    def _pkill(self, module_name, extra=None):
        pids = self._get_pid(module_name, extra)
        for pid in pids:
            self._stop(pid)

    def _wait_exit(self, module_name, pid):
        while True:
            if not self._get_pid(module_name, extra=['defunct', '%s' % pid]):
                break

    @staticmethod
    def _stop(pid=None):
        if pid is None:
            return
        cmd = 'kill -9 %d' % pid
        os.system(cmd)

    @staticmethod
    def _suspend(pid=None):
        if pid is None:
            return
        if isinstance(pid, int):
            cmd = 'kill -STOP %d' % pid
            os.system(cmd)
        if isinstance(pid, str):
            cmd = 'kill -STOP %s' % pid
            os.system(cmd)

    @staticmethod
    def _resume(pid=None):
        if pid is None:
            return
        if isinstance(pid, int):
            cmd = 'kill -CONT %d' % pid
            os.system(cmd)
        if isinstance(pid, str):
            cmd = 'kill -CONT %s' % pid
            os.system(cmd)

    def kill_stop(self):
        if hasattr(self, 'pid'):
            ModuleBase._stop(self.pid)
            self.pid = None
        if hasattr(self, 'pids'):
            for i in range(len(self.pids)):
                ModuleBase._stop(self.pids[i])
                self.pids[i] = None

    def suspend(self):
        if not self.suspend_flag:
            if hasattr(self, 'pid'):
                ModuleBase._suspend(self.pid)
                self.suspend_flag = True
            if hasattr(self, 'pids'):
                for pid in self.pids:
                    ModuleBase._suspend(pid)
                self.suspend_flag = True

    def resume(self):
        if self.suspend_flag:
            if hasattr(self, 'pid'):
                ModuleBase._resume(self.pid)
                self.suspend_flag = False
            if hasattr(self, 'pids'):
                for pid in self.pids:
                    ModuleBase._resume(pid)
                self.suspend_flag = False
