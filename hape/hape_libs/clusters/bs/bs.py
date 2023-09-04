import os
import base64
import json

from hape_libs.clusters.hippo_appmaster import *

class BsAdmin(HippoAppMasterBase):
    def __init__(self, conf_key, cluster_config, catalog_root_zk):
        super(BsAdmin, self).__init__(conf_key, cluster_config)
        self._catalog_root_zk = catalog_root_zk
    
    def admin_process_name(self):
        return "bs_admin_worker"
    
    def worker_process_name(self):
        return "build_service_worker"
    
    def processor_spec(self):

        binaryPath = self._cluster_config.get_domain_config_param(HapeCommon.COMMON_CONF_KEY, "binaryPath")
        envs = [
                "USER={}".format(self._user),
                "HOME={}".format(self._process_workdir_root),
                "/bin/env PATH={}/usr/local/bin:$PATH".format(binaryPath),
                "LD_LIBRARY_PATH=/usr/lib:/usr/lib64:/opt/taobao/java/jre/lib/amd64/server:{}/usr/local/lib:{}/usr/local/lib64".format(binaryPath, binaryPath),
                "HADOOP_HOME={}".format(self._hadoop_home),
                "JAVA_HOME={}".format(self._java_home),
                "control_flow_config_path={}/usr/local/etc/bs/lua/lua.conf".format(binaryPath),
                "MULTI_SLOTS_IN_ONE_NODE=true",
                "CUSTOM_CONTAINER_PARAMS={}".format(base64.b64encode(self.worker_docker_options().encode("utf-8")).decode("utf-8")),
        ]
        
        if self.processor_local_mode:
            envs += [
                "CMD_LOCAL_MODE=true",
                "CMD_ONE_CONTAINER=true"
            ]

        args = ["--app_name {}".format(self.service_name()),
                "--zookeeper {}".format(self.service_zk_full_path()),
                "--amonitor_port 10086",
                "--hippo {}".format(self.hippo_zk_full_path()), 
                "-l {}/usr/local/etc/bs/bs_admin_alog.conf".format(binaryPath),
                "-w .",
                "-d",
                "--catalog_address {} --catalog_name {}".format(self._catalog_root_zk, HapeCommon.DEFAULT_CATALOG)
            ]
        return ProcessorSpec(envs, args, self.admin_process_name())
        
    def hippo_cmd(self):
        return "bs_admin_worker"
    
    def get_admin_leader_address(self):
        path = os.path.join(self.service_zk_path(), "admin/LeaderElection/leader_election0000000000")
        Logger.debug("Try to parse admin leader from path {}".format(os.path.join(self._proc_zfs_wrapper.root_address, path)))
        data = self._proc_zfs_wrapper.get(path)
        return json.loads(data)["address"]
    
    def hippo_candidate_ips(self):
        return {"default": self._cluster_config.get_domain_config_param(HapeCommon.BS_KEY, "workerIpList").split(";")}
    
    def get_worker_process_status(self):
        result = super().get_worker_process_status()
        
        