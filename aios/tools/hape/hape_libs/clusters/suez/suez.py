import os
import base64
import json
import requests
import traceback

from hape_libs.clusters.hippo_appmaster import *
from hape_libs.utils.fs_wrapper import FsWrapper
from hape_libs.clusters.suez import *
from hape_libs.utils.logger import Logger
from hape_libs.config import CarbonHippoPlanParser
from hape_libs.utils.shell import SSHShell

class SuezAdmin(HippoAppMasterBase):

    def admin_process_name(self):
        return "suez_admin_worker"

    def worker_process_name(self):
        return "ha_sql"

    def processor_spec(self):
        binaryPath = self._cluster_config.get_domain_config_param(HapeCommon.COMMON_CONF_KEY, "binaryPath")
        envs = [
                "USER={}".format(self._user),
                "HOME={}".format(self._process_workdir_root),
                "/bin/env PATH={}/usr/local/bin:$PATH".format(binaryPath),
                "LD_LIBRARY_PATH=/usr/local/lib64/ssl/lib64:/usr/lib:/usr/lib64:/opt/taobao/java/jre/lib/amd64/server:{}/usr/local/lib:{}/usr/local/lib64".format(binaryPath, binaryPath),
                "DIRECT_WRITE_TEMPLATE_CONFIG_PATH={}".format(self._cluster_config.template_config.get_direct_write_template_path()),
                "HIPPO_LOCAL_SCHEDULE_MODE=true",
                "HADOOP_HOME={}".format(self._hadoop_home),
                "JAVA_HOME={}".format(self._java_home),
                "BS_TEMPLATE_CONFIG_PATH={}".format(self._cluster_config.template_config.get_bs_template_path()),
                "MULTI_SLOTS_IN_ONE_NODE={}".format(self._cluster_config.get_domain_config_param(HapeCommon.HAVENASK_KEY, "allowMultiSlotInOne")),
                "CUSTOM_CONTAINER_PARAMS={}".format(base64.b64encode(self.worker_docker_options().encode("utf-8")).decode("utf-8")),
        ]
        if self.processor_local_mode:
            envs += [
                "CMD_LOCAL_MODE=true",
                "CMD_ONE_CONTAINER=true"
            ]

        binaryPath = self._cluster_config.get_domain_config_param(HapeCommon.COMMON_CONF_KEY, "binaryPath")
        args = ["-a {}".format(self.service_name()),
                "-z {}".format(self.service_zk_full_path()),
                "-b {}".format("/".join([self.hippo_zk_full_path()])),
                "-l {}/usr/local/etc/suez/suez_alog.conf".format(binaryPath),
                "--httpPort {}".format(self._cluster_config.get_domain_config_param(HapeCommon.HAVENASK_KEY, "adminHttpPort")),
                "--opsMode",
                "--localCm2Mode",
                "-c {}/usr/local/etc/suez/server.cfg".format(binaryPath),
                "--storeUri {}".format(self.service_zk_full_path()),
                "-w .",
                "-d",
                "-n"
            ]
        
        extra_envs, extra_args = self._cluster_config.get_extra_envs_and_args(self._conf_key)
        envs += extra_envs
        args += extra_args
        return ProcessorSpec(envs, args, self.admin_process_name())


    def get_admin_leader_address(self):
        path = os.path.join(self.service_zk_path(), "admin/LeaderElection/leader_election0000000000")
        Logger.debug("Try to parse admin leader from path {}".format(os.path.join(self._proc_zfs_wrapper.root_address, path)))
        data = self._proc_zfs_wrapper.get(path)
        return data.split("\n")[-1]


    def hippo_candidate_ips(self):
        return {"qrs": self._cluster_config.get_domain_config_param(HapeCommon.HAVENASK_KEY, "qrsIpList").split(";"),
                "searcher": self._cluster_config.get_domain_config_param(HapeCommon.HAVENASK_KEY, "searcherIpList").split(";")}
    
    def stop(self, is_delete):
        super(SuezAdmin, self).stop(is_delete)
        ## temporary solution to update hippo config
        self._proc_zfs_wrapper.rm("{}/serialize_dir".format(self.service_zk_path()))
        self._proc_zfs_wrapper.rm("{}/cluster_state.pb".format(self.service_zk_path()))
        self._proc_zfs_wrapper.rm("{}/assigned_slots_info".format(self.hippo_zk_path()))
        if is_delete:
            FsWrapper(self._cluster_config.get_data_store_root(), extend_attrs={
                "binary_path": self._cluster_config.get_domain_config_param(HapeCommon.COMMON_CONF_KEY, "binaryPath"),
                "hadoop_home": self._hadoop_home
                }).rm("")


    def is_ready(self):
        try:
            address = self.get_admin_leader_address()
            response = requests.post("http://{}/CatalogService/listCatalog".format(address))
            data = response.json()
            Logger.debug("Catalog service response: {}".format(str(data)))
            if len(data['status']) != 0:
                Logger.debug("Catalog service is not ready in suez admin by now")
                return False

            response = requests.post("http://{}/ClusterService/getClusterDeployment".format(address))
            data = response.text
            Logger.debug("Cluster service response: {}".format(str(data)))
            if data.find("SERVICE_NOT_READY") == -1:
                return True
            else:
                Logger.debug("Cluster service is not ready in suez admin by now")
                return False

        except Exception as e:
            return False

        
    def get_qrs_httpport(self):
        qrs_hippo = self._cluster_config.template_config.get_local_template_content("havenask/hippo/qrs_hippo.json")
        qrs_hippo = json.loads(qrs_hippo)
        qrshttpPort = CarbonHippoPlanParser(qrs_hippo).parse_args_env("httpPort")
        return qrshttpPort
    
    def parse_qrs_ip(self):
        result = []
        for worker_info in self.get_assigned_hippo_worker_infos(role="qrs"):
            result.append(worker_info.ip)
        return result

    def get_qrs_ips(self):
        qrs_hippo = self._cluster_config.template_config.get_local_template_content("havenask/hippo/qrs_hippo.json")
        qrs_hippo = json.loads(qrs_hippo)
        qrs_count = CarbonHippoPlanParser(qrs_hippo).parse_count()
                
        def check_qrs_replicas():
            qrs_worker_ips = self.parse_qrs_ip()
            if len(qrs_worker_ips) != qrs_count:
                Logger.info("Qrs expect to have {} replicas started, now {}".format(qrs_count, len(qrs_worker_ips)))
                return False
            else:
                return True
        check_msg = "qrs achieve target replicas"
        ready = Retry.retry(check_qrs_replicas, check_msg, 50)
        if not ready:
            return None
        else:
            return self.parse_qrs_ip()
    
    def collect_qrs_tables(self):
        qrs_worker_ips = self.get_qrs_ips()
        if qrs_worker_ips == None:
            return False
        
        Logger.debug("Start to collect table in all qrs")
        Logger.debug("Qrs iplist: {}".format(qrs_worker_ips))
        
        qrshttpPort = self.get_qrs_httpport()
        
        tables_collect = {}
        for ip in qrs_worker_ips:
            try:
                response = requests.post("http://{}:{}/sqlClientInfo".format(ip, qrshttpPort))
                data = response.json()
                tables = data['result']["default"][HapeCommon.DEFAULT_DATABASE]['tables']
                for table in tables:
                    if table not in tables_collect:
                        tables_collect[table] = []
                    tables_collect[table].append(ip)  
            except Exception as e:
                # Logger.info(traceback.format_exc())
                pass
            
        return qrs_worker_ips, tables_collect
    
    def get_ready_tables(self):
        qrs_worker_ips, tables_collect = self.collect_qrs_tables()
        ready_tables = set()
        for table in tables_collect:
            if len(tables_collect[table]) == len(qrs_worker_ips):
                ready_tables.add(table)
        return ready_tables
    
    
    def clean_tmp_table_config(self):
        container_infos = self.get_assigned_hippo_worker_infos()
        for container_info in container_infos:
            shell = SSHShell(container_info.ip)
            shell.execute_command("rm -rf {}_{}/ha_sql/zone_config/table_temp_config".format(self.service_name(), container_info.role))