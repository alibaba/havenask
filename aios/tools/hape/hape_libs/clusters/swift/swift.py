import os
import base64
import json

from hape_libs.config import *
from hape_libs.container import *
from hape_libs.clusters.hippo_appmaster import *
from hape_libs.utils.logger import Logger
from hape_libs.utils.retry import Retry


class SwiftAdmin(HippoAppMasterBase):
    def __init__(self, key, cluster_config):
        super(SwiftAdmin, self).__init__(key, cluster_config)
        self.swift_tool = SwiftTool(cluster_config,self.container_service, self)
        self._config_version = None
        self._proc_zfs_wrapper = FsWrapper(self._cluster_config.get_domain_config_param(HapeCommon.SWIFT_KEY, "swiftZkRoot"), "zfs")
        
        
    def admin_process_name(self):
        return "swift_admin"
    
    def worker_process_name(self):
        return "swift_broker"

    def processor_spec(self):
        cmd = self.admin_process_name()
        binaryPath = self._cluster_config.get_domain_config_param(HapeCommon.COMMON_CONF_KEY, "binaryPath")
        envs = [
            "USER={}".format(self._user),
            "HOME={}".format(self._process_workdir_root),
            "PATH={}/usr/local/bin:$PATH".format(binaryPath),
            "LD_LIBRARY_PATH=/usr/local/lib64/ssl/lib64:/usr/lib:/usr/lib64:/opt/taobao/java/jre/lib/amd64/server:{}/usr/local/lib:{}/usr/local/lib64".format(binaryPath, binaryPath),
            "HADOOP_HOME={}".format(self._hadoop_home),
            "JAVA_HOME={}".format(self._java_home),
            "HIPPO_LOCAL_SCHEDULE_MODE=true",
            "MULTI_SLOTS_IN_ONE_NODE=true",
            "CUSTOM_CONTAINER_PARAMS={}".format(base64.b64encode(self.worker_docker_options().encode("utf-8")).decode("utf-8"))
        ]
        if self.processor_local_mode:
            envs += [
                "CMD_LOCAL_MODE=true",
                "CMD_ONE_CONTAINER=true"
            ]
        
        zk_full_path = self.service_zk_full_path()
        binaryPath = self._cluster_config.get_domain_config_param(HapeCommon.COMMON_CONF_KEY, "binaryPath")
        args = ["-c {}/config/{}".format(zk_full_path, self._config_version),
                "-d",
                "-l {}/usr/local/etc/swift/swift_alog.conf".format(binaryPath),
                "-w ."]
        
        extra_envs, extra_args = self._cluster_config.get_extra_envs_and_args(self._conf_key)
        envs += extra_envs
        args += extra_args
        return ProcessorSpec(envs, args, cmd)
        
    def get_admin_leader_address(self):
        path = os.path.join(self.service_zk_path(), "admin/leader_info.json")
        data = self._proc_zfs_wrapper.get(path)
        leader_info = json.loads(data.strip())
        return leader_info["address"]
        
    def hippo_candidate_ips(self):
        return {"default": self._cluster_config.get_domain_config_param(self._conf_key, "workerIpList").split(";")}
    
    def _start_process(self, container_meta):
        ## deploy config
        version, succ = self.swift_tool.deploy_config(ContainerMeta(self.container_name(), container_meta.ip))
        if not succ:
            return False
        self._config_version = version
        
        zk_full_path = os.path.join(self._proc_zfs_wrapper.root_address, self.service_zk_path())
        config_zk_full_path = "/".join([zk_full_path, "config"])
        
        ## create version file in zk
        version_info = {
            "rollback": False,
            "admin_current": "/".join([config_zk_full_path, version]),
            "broker_target_role_version": "0",
            "broker_current":  "/".join([config_zk_full_path, version]),
            "broker_target":  "/".join([config_zk_full_path, version]),
            "broker_current_role_version": "0"
        }
        
        version_zk_path = os.path.join(self.service_zk_path(), "config/version")
        if self._proc_zfs_wrapper.exists(version_zk_path):
            self._proc_zfs_wrapper.rm(version_zk_path)

        self._proc_zfs_wrapper.write(json.dumps(version_info), version_zk_path)
        
        return super(SwiftAdmin, self)._start_process(container_meta)
    
    def is_ready(self):
        try:
            return self.swift_tool.check_admin_alive()
        except:
            Logger.warning("Swift admin is not ready")
            
    def worker_container_tag(self):
        swift_conf = self._cluster_config.template_config.get_local_template_content("swift/config/swift.conf")
        user_name = ""
        for line in swift_conf.split():
            if line.find("=") != -1:
                k,v = line.split("=")
                if k == "user_name":
                    user_name = v
        return user_name + "_" + super(SwiftAdmin, self).worker_container_tag()
        
        

class SwiftTool():
    def __init__(self, cluster_config, container_service , swift_admin_processor):
        self._container_service = container_service
        self._cluster_config = cluster_config
        self._swift_admin_processor = swift_admin_processor
        self._container_service = container_service
        self._binaryPath = cluster_config.get_domain_config_param(HapeCommon.COMMON_CONF_KEY, "binaryPath")
        self._swift_zk_path = swift_admin_processor.service_zk_full_path()
        
    def deploy_config(self, container_meta):
        download_path, succ = self._transfer_config(container_meta)
        if not succ:
            Logger.error("Deploy swift config failed")
            return "", False
        
        argstr = "dp -z {} -l {}".format(self._swift_zk_path, download_path)
        out, flag = self.wrapped_execute(argstr, admin_ip=container_meta.ip, grep_text="success")
        
        if flag:
            hint = "max version is "
            offset = out.find(hint)
            if offset != -1:
                version = out[offset + len(hint):]
                Logger.info("Deploy swift config succeed")
                return version.strip(), True
        else:
            Logger.error("Failed to execute swift tools dp command, detail:{}".format(out))
            
        Logger.error("Deploy swift config failed")
        return "", False
        
    def add_topic(self, topic_name, partiton_count):
        
        argstr = "at -d -z {} -t {} -c {} -y 1024 -r 30 -m security_mode".format(self._swift_zk_path, topic_name, partiton_count)
        add_out, succ = self.wrapped_execute(argstr, grep_text="success")
        if not succ:
            Logger.warning("Add swift Topic response: {}".format(add_out))
        
        def _try_add_topic():
            argstr = "gti -z {} -t {}".format(self._swift_zk_path, topic_name)
            status_out, succ = self.wrapped_execute(argstr, grep_text="TopicStatus: RUNNING")
            Logger.info("Get swift topic status Response: {}".format(status_out))
            Logger.info("Swift Topic status is not ready")
            return succ
        
        check_msg = "swift topic created and ready"
        return Retry.retry(_try_add_topic, check_msg)
    
    def delete_topic(self, topic_name):
        argstr = "dt -z {} -t {}".format(self._swift_zk_path, topic_name)
        out = self.wrapped_execute(argstr)
        succ = (out.find("success") != -1 or out.find("ERROR_ADMIN_TOPIC_NOT_EXISTED") != -1)
        if not succ:
            Logger.warning("Failed to delete swift topic, detail: {}".format(out))
    
    def check_admin_alive(self):
        argstr = "gs -z {} ".format(self._swift_zk_path)
        out, alive = self.wrapped_execute(argstr, grep_text="Status[ALIVE]")
        return alive
    
    def get_partition_count(self, topic_name):
        info = self.wrapped_execute("gettopicinfo -z {} -t {}".format(self._swift_admin_processor.service_zk_path(), topic_name))
        for line in info.split("\n"):
            if ":" in line:
                splits = line.split(":")
                key = splits[0]
                if key == "PartitionCount":
                    return int(":".join(splits[1:]))
     
    def wrapped_execute(self, argstr, admin_ip = None, grep_text = None):
        if admin_ip == None:
            ip = self._swift_admin_processor.get_admin_leader_address().split(":")[0]
        else:
            ip = admin_ip
        container_name = self._swift_admin_processor.container_name()
        container_meta = ContainerMeta(container_name, ip)
        
        envs = [
            "PYTHONPATH={}/usr/local/lib/python/site-packages:$PYTHONPATH".format(self._binaryPath),
            "PATH={}/usr/local/bin:$PATH".format(self._binaryPath),
        ]
        cmd = "swift"
        process_spec = ProcessorSpec(envs, argstr.split(" "), cmd)
        return self._container_service.execute_command(container_meta, process_spec, grep_text = grep_text)
            
    def _transfer_config(self, container_meta):
        self._write_config_to_zk()
        download_path, succ = self._download_config_from_zk(container_meta)
        if not succ:
            Logger.error("Download config to container failed")
            return download_path, False
        return download_path, True
    
    def _write_config_to_zk(self):
        zfs_wrapper = FsWrapper(self._swift_zk_path)
        files = ["swift/config/swift.conf", "swift/config/swift_hippo.json"]
        for file in files:
            Logger.info("Start to write {} to zk".format(file))
            # with open(src_path + "/{}".format(file)) as f:
            #     swift_conf = f.read()
            swift_conf = self._cluster_config.template_config.get_local_template_content(file)
            swift_config_zkpath = "{}.__temp__".format(file.split("/")[-1])
            if zfs_wrapper.exists(swift_config_zkpath):
                zfs_wrapper.rm(swift_config_zkpath)
            zfs_wrapper.write(swift_conf, swift_config_zkpath)
    
    def _download_config_from_zk(self, container_meta):
        envs = [
            "LD_LIBRARY_PATH={}/usr/local/lib:{}/usr/local/lib64".format(self._binaryPath, self._binaryPath)
        ]
        cmd = "{}/usr/local/bin/fs_util".format(self._binaryPath)
        files = ["swift.conf", "swift_hippo.json"]
        download_path = "swift_temp_config"
        processor_spec = ProcessorSpec({}, {}, "rm -rf {}".format(download_path))
        self._container_service.execute_command(container_meta, processor_spec)

        mkdir_proc = ProcessorSpec({}, {}, "mkdir {}".format(download_path))
        self._container_service.execute_command(container_meta, mkdir_proc)
        
        check_proc = ProcessorSpec({}, {}, "ls | grep {}".format(download_path))
        out, exists = self._container_service.execute_command(container_meta, check_proc, grep_text=download_path)
        if not exists:
            return "", False
            
        for file in files:
            Logger.info("Start to download {} from zk in container".format(file))
            swift_config_zkpath = self._swift_zk_path + "/{}.__temp__".format(file)
            args = [
                "cp {} {}".format(swift_config_zkpath, download_path + "/{}".format(file))
            ]
            processor_spec = ProcessorSpec(envs, args, cmd)
            out, succ = self._container_service.execute_command(container_meta, processor_spec, grep_text="success")
            if not succ:
                Logger.info("Failed to download config from zk, detail: {}".format(out))
                return "", False
        return download_path, True

    
    
