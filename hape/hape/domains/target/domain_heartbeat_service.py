import os
from hape.utils.dict_file_util import DictFileUtil
from hape.utils.shell import Shell
from hape.utils.shell import Shell
from hape.utils.logger import Logger

from .target import Target
import time
from hape.common.constants.hape_common import HapeCommon
import shutil
import datetime
import traceback

'''
wrapper to read and write target and heartbeat
'''

class DistributeTargetError(Exception):
    pass

class DomainHeartbeatService:
    
    def __init__(self, global_config, supress_warning=True):
        self._workdir = os.path.join(global_config["hape-admin"]["workdir"], HapeCommon.ADMIN_DOMAIN_DIR, HapeCommon.HB_WORKDIR)
        self._worker_workdir = os.path.join(global_config["hape-worker"]["workdir"])
        self._supress_warning = supress_warning

    
    def get_workerlist(self, domain_name, role_name):
        workers_dir = os.path.join(self._workdir, domain_name, role_name)
        # Logger.info("try to get workers from {}".format(workers_dir))
        if not os.path.exists(workers_dir):
            return []
        return [filename for filename in os.listdir(workers_dir)]
    
    def get_worker_target_file_path(self, domain_name, role_name, worker_name, type):
        target_dir = os.path.join(self._workdir, domain_name, role_name, worker_name)
        target_file_path = os.path.join(target_dir, worker_name + "-" + type + ".json")
        return target_file_path
    
    def _read_worker_target(self, domain_name, role_name, worker_name, type):
        try:
            target_file_path = self.get_worker_target_file_path(domain_name, role_name, worker_name, type)
            return DictFileUtil.read(target_file_path, supress_warning= self._supress_warning)
        except:
            if not self._supress_warning:
                Logger.warning("{} target for worker {} not exists in {}".format(type, worker_name, target_file_path))
            return None
    
    def _write_worker_target(self, domain_name, role_name, worker_name, type, dict_obj):
        target_file_path = self.get_worker_target_file_path(domain_name, role_name, worker_name, type)
        target_dir = os.path.dirname(target_file_path)
        if not os.path.exists(target_dir):
            shell=Shell()
            shell.makedir(target_dir)
        dict_obj[HapeCommon.LAST_TARGET_TIMESTAMP_KEY[type]] = str(datetime.datetime.now())
        Logger.debug("write worker target, path:{} target:{}".format(target_file_path, dict_obj))
        return DictFileUtil.write(target_file_path, dict_obj)
    
    
    def remove_worker_targetdir(self, domain_name, role_name, worker_name, stop_mode=False):
        target_dir = os.path.join(self._workdir, domain_name, role_name, worker_name)
        Logger.info("clean worker {} workdir {}".format(worker_name, target_dir))
        if os.path.exists(target_dir):
            if os.path.isdir(target_dir):
                if not stop_mode:
                    shutil.rmtree(target_dir)
                else:
                    for filename in os.listdir(target_dir):
                        if os.path.isfile(os.path.join(target_dir, filename)) and filename.find("final-target")==-1:
                            os.remove(os.path.join(target_dir, filename))
            else:
                os.remove(target_dir)
    
    def _remove_worker_target(self, domain_name, role_name, worker_name, type):
        target_file_path = self.get_worker_target_file_path(domain_name, role_name, worker_name, type)
        if os.path.exists(target_file_path):
            Logger.info("worker target is remove, path:{} type:{}".format(target_file_path, type))
            os.remove(target_file_path)
    
    def read_worker_user_target(self, domain_name, role_name, worker_name):
        return self._read_worker_target(domain_name, role_name, worker_name, Target.USER_TARGET_TYPE)
    
    def write_worker_user_target(self, domain_name, role_name, worker_name, dict_obj):
        Logger.info("target for worker {} is saved, would be processed by domain daemon asynchronously in backgourd".format(worker_name))
        return self._write_worker_target(domain_name, role_name, worker_name, Target.USER_TARGET_TYPE, dict_obj)
    
    def remove_worker_user_target(self, domain_name, role_name, worker_name):
        return self._remove_worker_target(domain_name, role_name, worker_name, Target.USER_TARGET_TYPE)
    
    def read_worker_final_target(self, domain_name, role_name, worker_name):
        return self._read_worker_target(domain_name, role_name, worker_name, Target.FINAL_TARGET)
    
    def write_worker_final_target(self, domain_name, role_name, worker_name, dict_obj):
        return self._write_worker_target(domain_name, role_name, worker_name, Target.FINAL_TARGET, dict_obj)
    
    def read_worker_heartbeat(self, domain_name, role_name, worker_name):
        return self._read_worker_target(domain_name, role_name, worker_name, Target.HEARTBEAT)
    
    def write_worker_heartbeat(self, domain_name, role_name, worker_name, dict_obj):
        return self._write_worker_target(domain_name, role_name, worker_name, Target.HEARTBEAT, dict_obj)
    
    def remove_worker_heartbeat(self, domain_name, role_name, worker_name):
        return self._remove_worker_target(domain_name, role_name, worker_name, Target.HEARTBEAT)

    
    def distribute_target(self, final_target): #type:(Target) -> None
        processor_info = final_target['plan'][HapeCommon.PROCESSOR_INFO_KEY]
        domain_name, rolename, worker_name, ipaddr = processor_info["domain_name"], processor_info["role_name"], processor_info["worker_name"], processor_info["ipaddr"]
        from_path = self.get_worker_target_file_path(domain_name, rolename, worker_name, Target.FINAL_TARGET)
        to_path = os.path.join(self._worker_workdir, worker_name, HapeCommon.HB_WORKDIR)
        # Logger.info("final target {}".format(final_target))
        Logger.info("try to distribute target to {}, ip {}".format(final_target['worker_name'], ipaddr))
        try:
            shell = Shell(ipaddr)
            if not shell.file_exists(to_path):
                shell.makedir(to_path)
            shell = Shell()
            shell.put_remote_file(from_path, ipaddr+":"+to_path)
            Logger.info("distribute target to {} succeed {}".format(final_target['worker_name'], final_target))
        except:
            Logger.error("failed to distribute target")
            Logger.error(traceback.format_exc())
            raise DistributeTargetError
    
    def collect_heartbeat(self, domain_name, role_name, worker_name):
        final_target = self.read_worker_final_target(domain_name, role_name, worker_name)
        ipaddr = final_target["plan"][HapeCommon.PROCESSOR_INFO_KEY]["ipaddr"]
        from_path = os.path.join(self._worker_workdir, worker_name, "heartbeats", worker_name+"-heartbeat.json")
        to_path = os.path.dirname(self.get_worker_target_file_path(domain_name, role_name, worker_name, Target.HEARTBEAT))
        Logger.info("collect heartbeat from {}:{} to {}".format(ipaddr, from_path, to_path))
        shell=Shell()
        if not os.path.exists(to_path):
            shell.makedir(to_path)
        tmp_to_path =  os.path.join(to_path, "__temp_heartbeat.json")
        try:
            shell.get_remote_file(ipaddr + ":" + from_path, tmp_to_path)
            heartbeat =  DictFileUtil.read(os.path.join(tmp_to_path))
        except:
            heartbeat = None
            Logger.warning("heartbeat not exists in host:{}, path:{}".format(ipaddr, from_path))

        try:        
            heartbeat =  DictFileUtil.read(os.path.join(tmp_to_path))
        except:
            heartbeat = None
        if heartbeat!=None and len(heartbeat) != 0:
            self.write_worker_heartbeat(domain_name, role_name, worker_name, heartbeat)