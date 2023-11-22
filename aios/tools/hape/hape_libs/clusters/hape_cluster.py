import json

from hape_libs.clusters.suez import *
from hape_libs.clusters.swift import SwiftAdmin
from hape_libs.clusters.bs import BsAdmin
from hape_libs.common import HapeCommon
from hape_libs.utils.logger import Logger
from hape_libs.container import *
from hape_libs.config import *
from hape_libs.clusters.infra import InfraManager
from hape_libs.utils.retry import Retry
import time
import json
from collections import OrderedDict

class HapeCluster(object):
    def __init__(self, cluster_config): #type:(ClusterConfig)->None
        self.cluster_config = cluster_config
        self.infra_manager = InfraManager(self.cluster_config)
        if self.infra_manager.keep() == False:
            raise RuntimeError("Failed to keep infrastructure of havenask")
        self.swift_appmaster = SwiftAdmin(HapeCommon.SWIFT_KEY, self.cluster_config)
        self.suez_appmaster = SuezAdmin(HapeCommon.HAVENASK_KEY, self.cluster_config)
        self.swift_tool = self.swift_appmaster.swift_tool

        if cluster_config.has_bs_domain_config():
            self.bs_appmaster = BsAdmin(HapeCommon.BS_KEY, self.cluster_config, self.suez_appmaster.service_zk_full_path())
        else:
            self.bs_appmaster = None

        self.appmaster_dict = {
            HapeCommon.SWIFT_KEY: self.swift_appmaster,
            HapeCommon.BS_KEY: self.bs_appmaster,
            HapeCommon.HAVENASK_KEY: self.suez_appmaster,
        }

    def restart_havenask_admin(self):
        key = HapeCommon.HAVENASK_KEY
        Logger.info("Begin to clean temp table config".format(key))
        self.suez_appmaster.clean_tmp_table_config()
        Logger.info("Clean temp table config succeed".format(key))
        Logger.info("Begin to restart {} admin".format(key))
        for ip in self.cluster_config.get_admin_ip_list(key):
            succ = self.suez_appmaster.restart_container(ContainerMeta(self.suez_appmaster.container_name(), ip))
            if not succ:
                Logger.error("Failed to restart admin in ip: {}".format(ip))
                return False
        return True

    def start_hippo_appmaster(self, key,  allow_restart=False):
        start_havenask = (key == HapeCommon.HAVENASK_KEY)

        if start_havenask:
            if not self.cluster_config.swift_zk_set and not self.swift_appmaster.is_ready():
                Logger.info("SwiftZkRoot is not set and swift admin is not ready, begin to start default swift cluster")
                succ = self.start_hippo_appmaster(HapeCommon.SWIFT_KEY)
                if not succ:
                    Logger.error("Failed to prepare swift for havenask")
                    return False

        appmaster = self.appmaster_dict[key]

        cpu, mem, image = self.cluster_config.get_domain_config_param(key, "adminCpu"), \
            self.cluster_config.get_domain_config_param(key, "adminMem"), self.cluster_config.get_domain_config_param(key, "image")

        if allow_restart:
            Logger.info("Stop already existing admin and workers")
            self.appmaster_dict[key].stop(False)
        Logger.info("Begin to start {} admin".format(key))
        for ip in self.cluster_config.get_admin_ip_list(key):
            succ = appmaster.start(ContainerMeta(appmaster.container_name(), ip), ContainerSpec(image, cpu, mem), allow_restart = allow_restart)
            if not succ:
                Logger.error("Failed to create admin in ip: {}".format(ip))
                return False

        if start_havenask:
            self._init_havenask_cluster()

            if self.bs_appmaster != None:
                Logger.info("Detect bs config, begin to start bs admin")
                succ = self.start_hippo_appmaster(HapeCommon.BS_KEY, allow_restart=allow_restart)
                if not succ:
                    Logger.error("Failed to prepare bs for havenask")

        Logger.info("End to start {} admin".format(key))
        return True

    def stop_hippo_appmaster(self, key, is_delete=False):
        if is_delete and key == HapeCommon.HAVENASK_KEY:
            try:
                catalog_manager = self.get_suez_catalog_manager()
                if catalog_manager != None:
                    table_infos = catalog_manager.list_table()
                    for table in table_infos:
                        Logger.info("Delete swift topic {}".format(table))
                        table_infos = catalog_manager.list_table()
                        self.swift_tool.delete_topic(table)
            except:
                Logger.warning("Failed to delete swift topic")


        appmaster = self.appmaster_dict[key]
        appmaster.stop(is_delete)

        if key == HapeCommon.HAVENASK_KEY and self.cluster_config.has_bs_domain_config():
            Logger.info("Detect bs config in global.conf")
            self.appmaster_dict[HapeCommon.BS_KEY].stop(is_delete)

        return True

    def create_table(self, table, partition, schema, full_data_path):

        is_direct_table = (full_data_path == None)
        build_type = "DIRECT" if is_direct_table else "OFFLINE"

        catalog_manager = self.get_suez_catalog_manager()
        if catalog_manager == None:
            return False

        if table in catalog_manager.list_table_names():
            Logger.error("table {} already created".format(table))
            return False

        if self.bs_appmaster == None:
            Logger.error("Bs config is not in global.conf and bs admin is not started, cannot create offline type table")
            Logger.error("Please add bs config and execute subcommand: restart havenask")
            return False
        else:
            succ = catalog_manager.create_table(table, partition, schema, full_data_path, self.bs_appmaster.service_zk_full_path())
            if not succ:
                return False

        Logger.info("start to create swift topic {}".format(table))
        swift_tool = self.swift_tool
        is_succ = swift_tool.add_topic(table, partition)
        if not is_succ:
            Logger.error("Add swift topic failed")
            return False
        Logger.info("Succeed to create swift topic {}".format(table))

        if is_direct_table:
            Logger.info("{} type table need to be checked ready".format(build_type))
            Logger.debug("Table request is already sent, you can wait or use gs subcommand to check table status")

            def check_table_ready():
                return table in self.suez_appmaster.get_ready_tables()

            msg = "Table {} ready in all qrs".format(table)
            succ = Retry.retry(check_table_ready, msg, limit=50)
            if succ:
                Logger.info("Table {} is ready".format(table))
                Logger.info("Before read & write table {}, please confirm havenask cluster ready by using gs havenask subcommand".format(table))
                return True
            else:
                return False

        else:
            def try_get_build():
                try:
                    build_info = catalog_manager.get_build(table)
                    Logger.info("BuildService will keep build task for table, detail:{}".format(build_info))
                    return True
                except:
                    return False
            succ = Retry.retry(try_get_build, check_msg="buildservice generate build task", limit=10)
            if not succ:
                return False
            else:
                Logger.warning("{} type table may takes long time to build, will not be checked ready".format(build_type))
                return True

    def update_table_schema(self, table, partition, schema, full_data_path):
        is_direct_table = (full_data_path == None)
        if is_direct_table:
            Logger.error("Only support update table structure for offline mode, data_full_path should be specified")
            return False
        catalog_manager = self.get_suez_catalog_manager()
        if catalog_manager == None:
            return False
        if table not in catalog_manager.list_table_names():
            Logger.error("Table {} not created, create first".format(table))
            return False
        if self.bs_appmaster == None:
            Logger.error("Bs config is not in global.conf and bs admin is not started, "
                         "cannot create offline type table")
            Logger.error("Please add bs config and execute subcommand: restart havenask")
            return False
        else:
            succ = catalog_manager.update_table_schema(table, partition, schema, full_data_path,
                                                       self.bs_appmaster.service_zk_full_path())
            if not succ:
                return False
        return True

    def delete_table(self, table, keeptopic=False):
        catalog_manager = self.get_suez_catalog_manager()
        if catalog_manager == None:
            return False


        all_catalog_tables = catalog_manager.list_table_names()
        if table not in all_catalog_tables:
            Logger.error("table {} not exists".format(table))
            return False

        catalog_manager.delete_table(table)

        if not keeptopic:
            self.swift_tool.delete_topic(table)

        if len(all_catalog_tables) == 1:
            Logger.info("Delete last table, will not check ready")
            return True
        else:
            def check_table_delete():
                _, collect_tables = self.suez_appmaster.collect_qrs_tables()
                return table not in collect_tables

            msg = "table delete in all qrs"
            # succ = Retry.retry(check_table_delete, msg, limit=50)
            Logger.info("Succeed to delete table")
            Logger.info("Before continue to read & write tables, please confirm havenask cluster ready by using gs havenask subcommand".format(table))
            return True

    def update_load_strategy(self, table, online_index_config):
        catalog_manager = self.get_suez_catalog_manager()
        if catalog_manager == None:
            return False
        all_catalog_tables = catalog_manager.list_table_names()
        if table not in all_catalog_tables:
            Logger.error("table {} not exists".format(table))
            return False
        try:
            json.loads(online_index_config)
        except:
            Logger.error("invalid online index config json str: [{}] ".format(online_index_config))
            return False
        return catalog_manager.update_load_strategy(table, online_index_config)

    def create_or_update_load_strategy(self, table, online_index_config):
        catalog_manager = self.get_suez_catalog_manager()
        all_load_strategies = catalog_manager.list_load_strategy()
        if table not in all_load_strategies:
            Logger.debug("load strategy for table {} not exist, will create".format(table))
            return catalog_manager.create_load_strategy(table, online_index_config)
        else:
            return self.update_load_strategy(table, online_index_config)

    def get_suez_catalog_manager(self):
        if self.suez_appmaster.is_ready() and self.swift_appmaster.is_ready():
            suez_address = self.suez_appmaster.get_admin_leader_address()
            catalog_manager = CatalogManager("http://" + suez_address, self.swift_appmaster.service_zk_full_path(), self.cluster_config)
            return catalog_manager
        else:
            Logger.error("Suez or swift admin crashed or not ready")
            Logger.error("Please use `hape gs swift` and `hape gs havenask` to check which is abnormal and find errors in relevant logs")
            return None

    def get_suez_cluster_manager(self):
        if self.suez_appmaster.is_ready() and self.swift_appmaster.is_ready():
            suez_address = self.suez_appmaster.get_admin_leader_address()
            suez_cluster_manager = SuezClusterManager("http://" + suez_address,
                                                 self.swift_appmaster.service_zk_full_path(),
                                                 self.suez_appmaster.service_zk_full_path(),
                                                 self.cluster_config)
            return suez_cluster_manager
        else:
            Logger.error("Suez or swift admin crashed or not ready")
            Logger.error("Please use `hape gs swift` and `hape gs havenask` to check which is abnormal and find errors in relevant logs")
            return None

    def check_deployment(self):
        if self.suez_appmaster.is_ready() and self.swift_appmaster.is_ready():
            suez_address = self.suez_appmaster.get_admin_leader_address()
            suez_cluster_manager = SuezClusterManager("http://" + suez_address,
                                                 self.swift_appmaster.service_zk_full_path(),
                                                 self.suez_appmaster.service_zk_full_path(),
                                                 self.cluster_config)
            return suez_cluster_manager.check_deployment()
        else:
            return False

    def get_suez_scheduler_service(self):
        if self.suez_appmaster.is_ready():
            suez_address = self.suez_appmaster.get_admin_leader_address()
            scheduler_service = SchedulerService("http://" + suez_address)
            return scheduler_service
        else:
            Logger.error("Suez admin is not ready , cannot get scheduler service")
            return None


    def _init_havenask_cluster(self):
        catalog_manager, suez_cluster_manager = self.get_suez_catalog_manager(), self.get_suez_cluster_manager()
        if catalog_manager != None and suez_cluster_manager != None:
            catalog_manager.create_default_catalog()
            suez_cluster_manager.create_or_update_default_clusters()

    def update_default_cluster(self, hippo_config, role_type):
        suez_cluster_manager = self.get_suez_cluster_manager()
        suez_cluster_manager.update_default_cluster(hippo_config, role_type)

    def update_hippo_config(self, hippo_config_path, role_type):
        if not os.path.exists(hippo_config_path):
            Logger.error("hippo_config_path[{}] not exist", hippo_config_path)
            return None
        with open(hippo_config_path) as f:
            try:
                hippo_config = json.load(f)
            except Exception as e:
                Logger.error("hippo_config_path[{}] is not a valid json file", hippo_config_path)
                return None
        self.update_default_cluster(hippo_config, role_type)

    def get_default_hippo_config(self, role_type):
        suez_cluster_manager = self.get_suez_cluster_manager()
        hippo_config = suez_cluster_manager.get_default_hippo_config(role_type)
        print(json.dumps(hippo_config, indent=4))
        return hippo_config

    def get_sql_cluster_status(self, detail=False):
        scheduler_service = self.get_suez_scheduler_service()
        if scheduler_service == None:
            return None
        status = OrderedDict({})
        status["sqlClusterInfo"] = scheduler_service.get_status(detail)
        processor_status = self.suez_appmaster.get_all_processor_status()
        status.update(processor_status)
        print(json.dumps(status, indent=4))
        return status

    def get_swift_cluster_status(self):
        status = self.swift_appmaster.get_all_processor_status()
        print(json.dumps(status, indent=4))
        return status

    def get_bs_cluster_status(self):
        status = OrderedDict({})
        if not self.cluster_config.has_bs_domain_config():
            Logger.error("Bs Cluster is not started because there is no bs config in global.conf")
            return {}
        else:
            status = self.bs_appmaster.get_all_processor_status()

            catalog_manager = self.get_suez_catalog_manager()
            build_info = {}
            for name in catalog_manager.list_table_names():
                try:
                    build_info[name] = catalog_manager.get_build(name)
                except:
                    pass
            status['buildInfo'] = build_info

            print(json.dumps(status, indent=4))
            return status

    def get_table_status(self, specify_table_name=None):
        catalog_manager  = self.get_suez_catalog_manager()
        if catalog_manager != None:
            table_infos = catalog_manager.list_table()
            ready_tables = self.suez_appmaster.get_ready_tables()
            result = {}
            for table_name in table_infos:
                if specify_table_name != None and specify_table_name != table_name:
                    continue

                table_info = table_infos[table_name]
                if table_name in ready_tables:
                    table_info["status"] = "READY"
                else:
                    table_info["status"] = "NOT_READY"
                result[table_name] = table_info

            print(json.dumps(result, indent=4))
            return result

        else:
            return None


    def restart_container(self, ip, name):
        for key in self.appmaster_dict:
            if self.appmaster_dict[key] != None and name.find(self.appmaster_dict[key].service_name())!=-1:
                meta = ContainerMeta(name, ip)
                return self.appmaster_dict[key].restart_container(meta)

        Logger.error("Container {} not found".format(name))
        return False

    def stop_container(self, ip, name):
        for key in self.appmaster_dict:
            if self.appmaster_dict[key] != None and name.find(self.appmaster_dict[key].service_name())!=-1:
                meta = ContainerMeta(name, ip)
                return self.appmaster_dict[key].stop_container(meta)

        Logger.error("Container {} not found".format(name))
        return False

    def update_candidate_ips(self):
        Logger.info("Begin to update candidate ips")
        ##admin
        cluster_config = self.cluster_config
        for key in self.appmaster_dict:
            appmaster = self.appmaster_dict[key]
            container_service = appmaster.container_service
            for ip in cluster_config.get_admin_ip_list(key):
                container_meta = ContainerMeta(appmaster.container_name(), ip)
                processor_spec = appmaster.processor_spec()
                if container_service.check_process_pid(container_meta, processor_spec) == None:
                    appmaster.restart_container(container_meta)

        ##worker
        for key in self.appmaster_dict:
            if self.appmaster_dict[key] != None:
                self.appmaster_dict[key].write_candidate_iplist()

        Logger.info("Update candidate ips succeed")

    def wait_havenask_ready(self):
        def check_sqlcluster_ready():
            try:
                status = self.get_sql_cluster_status()
                return status["sqlClusterInfo"]["clusterStatus"] == "READY"
            except:
                return False

        succ = Retry.retry(check_sqlcluster_ready, "havenask cluster ready", limit=20)
        return succ

    def wait_new_generation_ready(self):
        status = self.get_sql_cluster_status(True)
        old_generation_ids = status["sqlClusterInfo"]['GenerationIds']
        def check_sqlcluster_ready():
            try:
                status = self.get_sql_cluster_status(True)
                cur_generation_ids = status["sqlClusterInfo"]['GenerationIds']
                return len(cur_generation_ids) > 0 and cur_generation_ids != old_generation_ids
            except:
                return False
        succ = Retry.retry(check_sqlcluster_ready, "new generation ready", limit=100)
        return succ

    def drop_build(self, name, generation_id):
        catalog_manager = self.get_suez_catalog_manager()
        try:
            catalog_manager.drop_build(name, generation_id)
        except:
            raise RuntimeError("Failed to drop build, table[{}], generation_id[{}]".format(name, generation_id))
