import os
import time
import traceback

from hape.utils.dict_file_util import DictFileUtil
from hape.utils.logger import Logger
from hape.utils.shell import Shell
from hape.domains.target.target import Target
from hape.common import HapeCommon
import datetime


class HeartBeatManager:
    HB_WORKDIR = "heartbeats"
    
    def __init__(self, worker_name, global_config):
        self._workdir = os.path.join(global_config["hape-worker"]["workdir"],  worker_name, HeartBeatManager.HB_WORKDIR)
    
    def get_worker_target_file_path(self, domain_name, role_name, worker_name, type):
        target_dir = self._workdir
        target_file_path = os.path.join(target_dir, worker_name + "-" + type + ".json")
        return target_file_path
    
    def _read_worker_target(self, domain_name, role_name, worker_name, type):
        try:
            target_file_path = self.get_worker_target_file_path(domain_name, role_name, worker_name, type)
            return DictFileUtil.read(target_file_path)
        except:
            Logger.warning("{} target for worker {} not exists in {}".format(type, worker_name, target_file_path))
            return None
    
    def _remove_worker_target(self, domain_name, role_name, worker_name, type):
        target_file_path = self.get_worker_target_file_path(domain_name, role_name, worker_name, type)
        if os.path.exists(target_file_path):
            Logger.info("worker target is consumed, path:{} content:{}".format(target_file_path, DictFileUtil.read(target_file_path)))
            os.remove(target_file_path)
    
    def read_worker_final_target(self, domain_name, role_name, worker_name):
        return self._read_worker_target(domain_name, role_name, worker_name, Target.FINAL_TARGET)
    
    def remove_worker_final_target(self, domain_name, role_name, worker_name):
        return self._remove_worker_target(domain_name, role_name, worker_name, Target.FINAL_TARGET)

    def _write_worker_target(self, domain_name, role_name, worker_name, type, dict_obj):
        target_file_path = self.get_worker_target_file_path(domain_name, role_name, worker_name, type)
        target_dir = os.path.dirname(target_file_path)
        if not os.path.exists(target_dir):
            shell=Shell()
            shell.makedir(target_dir)
        dict_obj[HapeCommon.LAST_TARGET_TIMESTAMP_KEY[type]] = str(datetime.datetime.now())
        return DictFileUtil.write(target_file_path, dict_obj)
        
    def write_worker_heartbeat(self, domain_name, role_name, worker_name, dict_obj):
        return self._write_worker_target(domain_name, role_name, worker_name, Target.HEARTBEAT, dict_obj)