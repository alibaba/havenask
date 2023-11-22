import os
import json
from collections import OrderedDict
import getpass

from hape_libs.container import *
from hape_libs.utils.fs_wrapper import FsWrapper
from hape_libs.common import *
from hape_libs.config import ClusterConfig
from hape_libs.utils.logger import Logger
from hape_libs.utils.retry import Retry

class HippoContainerInfo():
    def __init__(self, role, ip, container_name, processor_name):
        self.role = role
        self.ip = ip
        self.container_name = container_name
        self.processor_name = processor_name

    def to_dict(self):
        return OrderedDict({
            "role": self.role,
            "ip": self.ip,
            "containerName": self.container_name,
            "processorName": self.processor_name
        })

class HippoAssignedInfos():
    def __init__(self, container_tag, process_name, assigned_info): #type: (HippoAppMasterBase, str, str)->None
        self._container_infos = []
        for key, values in assigned_info.items():
            for value in values:
                valItems = value.split("&")
                ip = valItems[0]
                slot_idx = valItems[1]
                container_name = "havenask_container_" + container_tag + "_" +str(slot_idx)
                self._container_infos.append(HippoContainerInfo(key, ip, container_name, process_name))

    def get_container_infos(self, role=None):
        if role == None:
            return self._container_infos
        else:
            return [container for container in self._container_infos if container.role.find(role) != -1]

class HippoAppMasterBase(object):
    def __init__(self, conf_key, cluster_config): #type: (str, ClusterConfig) -> None
        self._conf_key = conf_key
        self._cluster_config = cluster_config

        if self._cluster_config.get_domain_config_param(HapeCommon.COMMON_CONF_KEY, "processorMode") == 'local':
            self.processor_local_mode = True
            self.container_service = LocalContainerService(cluster_config)
        else:
            self.processor_local_mode = False
            self.container_service = RemoteDockerContainerService(cluster_config)

        self._proc_zfs_wrapper = FsWrapper(self._cluster_config.get_domain_config_param(HapeCommon.COMMON_CONF_KEY, "domainZkRoot"), "zfs")
        self._current_homedir = os.path.expanduser("~")
        self._process_workdir_root = self._cluster_config.get_domain_config_param(HapeCommon.COMMON_CONF_KEY, "processorWorkdirRoot")
        self._user = getpass.getuser()
        self._uid = os.getuid()
        self._gid = os.getgid()
        self._hadoop_home = self._cluster_config.get_domain_config_param(HapeCommon.COMMON_CONF_KEY, "hadoopHome")
        self._java_home = self._cluster_config.get_domain_config_param(HapeCommon.COMMON_CONF_KEY, "javaHome")


    def admin_process_name(self):
        raise NotImplementedError

    def worker_process_name(self):
        raise NotImplementedError

    def worker_docker_options(self):
        worker_docker_options = "--volume=\"/etc/hosts:/etc/hosts:ro\" \
--ulimit nofile=655350:655350  --ulimit memlock=-1 --ulimit core=-1 --network=host --privileged -d "
        return worker_docker_options

    def processor_spec(self):
        raise NotImplementedError

    def service_name(self):
        return self._cluster_config.get_domain_config_param(self._conf_key, "serviceName")

    def container_name(self):
        return self.service_name() + "_appmaster"

    def worker_container_tag(self):
        return self.service_name()

    def service_zk_path(self):
        return self.service_name()

    def service_zk_full_path(self):
        return os.path.join(self._proc_zfs_wrapper.root_address, self.service_zk_path())

    def hippo_zk_path(self):
        return self.service_name() + "_hippo"

    def hippo_zk_full_path(self):
        return os.path.join(self._proc_zfs_wrapper.root_address, self.hippo_zk_path())

    def hippo_candidate_ips(self):
        raise NotImplementedError

    def get_admin_leader_address(self):
        raise NotImplementedError

    def is_ready(self):
        try:
            address = self.get_admin_leader_address()
            ip = address.split(":")[0]
            container_meta = ContainerMeta(self.container_name(), ip)
            return (self.container_service.check_process_pid(container_meta, self.processor_spec()) != None)
        except:
            # Logger.debug(traceback.format_exc())
            return False

    def start(self, container_meta, container_spec, allow_restart=False):
        self._cluster_config.template_config.upload()
        if self.is_ready():
            if not allow_restart:
                Logger.error("Admin {} already exists, please execute stop subcommands first or execute restart subcommands".format(self._conf_key))
                return False
            else:
                # Logger.info("Restart already existing admin and workers")
                # self.stop(False)
                pass

        if self._proc_zfs_wrapper.exists(self.service_zk_path()):
            Logger.info("Zk path {} already exists, will not change it".format(self.service_zk_full_path()))
        else:
            self._proc_zfs_wrapper.mkdir(self.service_zk_path())

        self.write_candidate_iplist()

        succ = self.container_service.create_container(container_meta, container_spec)
        if not succ:
            Logger.error("Create container failed")
            return False

        succ = self._start_process(container_meta)
        if not succ:
            Logger.error("Start process failed")
            return False


        def check_ready():
            return self.is_ready()

        check_msg = "{} admin ready".format(self._conf_key)
        return Retry.retry(check_ready, check_msg, 30)


    def stop(self, is_delete):
        Logger.info("Begin to stop {}".format(self.service_name()))

        for ip in self._cluster_config.get_admin_ip_list(self._conf_key):
            container_meta = ContainerMeta(self.container_name(), ip)
            if self.container_service.check_container(container_meta):
                self.container_service.stop_container(container_meta, self.admin_process_name())
        Logger.info("Succeed to stop admin")
        Logger.info("Start to stop workers")
        self._stop_workers()
        Logger.info("Succeed to stop {}".format(self.service_name()))


        if is_delete:
            Logger.info("Need to delete zookeepr and databse runtime files")
            zk_root = self.service_zk_path()
            Logger.info("Begin to clean zk {}".format(zk_root))
            if self._proc_zfs_wrapper.exists(zk_root):
                try:
                    self._proc_zfs_wrapper.rm(zk_root)
                except:
                    Logger.warning("Recusively clean zk failed, please clean by hand")

            zk_root = self.hippo_zk_path()
            Logger.info("Begin to clean zk {}".format(zk_root))
            if self._proc_zfs_wrapper.exists(zk_root):
                try:
                    self._proc_zfs_wrapper.rm(zk_root)
                except:
                    Logger.warning("Recusively clean zk failed, please clean by hand")

            Logger.info("Succeed to delete {}".format(self.service_name()))

    def write_candidate_iplist(self):
        candidate_path = os.path.join(self.hippo_zk_path(), "candidate_ips")
        if self._proc_zfs_wrapper.exists(candidate_path):
            Logger.warning("Candidate ips zk path {} already exists, will overwrite it".format(
                os.path.join(self._proc_zfs_wrapper.root_address, candidate_path)))

        candidate_ips = self.hippo_candidate_ips()
        self._proc_zfs_wrapper.write(json.dumps(candidate_ips), candidate_path)

    def _start_process(self, container_meta):
        succ = self.container_service.start_process(container_meta, self.processor_spec())
        return succ


    def _stop_workers(self):
        try:
            worker_infos = self.get_assigned_hippo_worker_infos()
            for worker_info in worker_infos:
                meta = ContainerMeta(worker_info.container_name, worker_info.ip)
                self.container_service.stop_container(meta, worker_info.processor_name)
        except:
            Logger.warning("Cannot parse assigned info for workers, skip stopping workers")


    def get_assigned_hippo_worker_infos(self, role=None):
        assigned_zk_path = os.path.join(self.hippo_zk_path(), "assigned_slots_info")
        info = self._proc_zfs_wrapper.get(assigned_zk_path).strip()
        assigned_info_map = json.loads(info)
        worker_infos = HippoAssignedInfos(self.worker_container_tag(), self.worker_process_name(), assigned_info_map).get_container_infos(role=role)
        return worker_infos


    def parse_admin_info(self):
        ipList = self._cluster_config.get_admin_ip_list(self._conf_key)
        address = self.get_admin_leader_address()
        return ipList, address


    def get_all_processor_status(self):
        admin_ip_list, leader_address = self.parse_admin_info()
        result = OrderedDict({"leaderAlive": self.is_ready(), "serviceZk": self.service_zk_full_path(), "hippoZk": self.hippo_zk_full_path(),
                              "leaderAddress": leader_address, "processors": []})
        processors_infos = self.get_assigned_hippo_worker_infos()
        for admin_ip in admin_ip_list:
            processors_infos = [HippoContainerInfo("admin", admin_ip, self.container_name(), self.admin_process_name())] + processors_infos


        for processor_info in processors_infos:
            processor_info_dict = processor_info.to_dict()
            result["processors"].append(processor_info_dict)
            ip, container_name, processor_name = processor_info.ip, processor_info.container_name, processor_info.processor_name
            container_meta = ContainerMeta(container_name, ip)
            exists = self.container_service.check_container(container_meta)
            processor_info_dict["containerStatus"] = "RUNNING" if exists else "NOT_RUNNING"
            pid = self.container_service.check_process_pid(container_meta, ProcessorSpec({}, {}, processor_name))
            processor_info_dict["processorStatus"] = "RUNNING" if pid != None else "NOT_RUNNING"
        return result

    def restart_container(self, container_meta):
        is_appmaster = (container_meta.name.find("appmaster") != -1)
        processor_name = self.admin_process_name() if is_appmaster else self.worker_process_name()
        if is_appmaster:
            cpu, mem, image = self._cluster_config.get_domain_config_param(self._conf_key, "adminCpu"), \
                self._cluster_config.get_domain_config_param(self._conf_key, "adminMem"), self._cluster_config.get_domain_config_param(self._conf_key, "image")
            container_spec = ContainerSpec(image, cpu, mem)
            ## update to the latest
            processor_spec = self.processor_spec()
            self.container_service.stop_container(container_meta, processor_spec.command)
            self.container_service.create_container(container_meta, container_spec)
            self.container_service.start_process(container_meta, processor_spec)
            def check_ready():
                return self.is_ready()
            succ = Retry.retry(check_ready, "{} admin ready".format(self._conf_key), limit=30)
            return succ
        else:
            ## will restart by admin
            succ = self.container_service.stop_container(container_meta, self.worker_process_name())
            Logger.info("Worker {} will restart by admin hippo scheulder, please wait".format(container_meta.name))
            return succ


    def stop_container(self, container_meta):
        succ = self.container_service.stop_container(container_meta, self.worker_process_name())
        return succ
