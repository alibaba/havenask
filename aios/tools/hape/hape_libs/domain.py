from functools import partial
import functools
import json

from hape_libs.clusters import *
from hape_libs.utils.logger import Logger
from hape_libs.config import *
from hape_libs.infra import InfraManager
from hape_libs.common import HapeCommon
from hape_libs.utils.retry import Retry
import json

class HavenaskDomain(object):
    def __init__(self, domain_config): #type:(DomainConfig)->None
        self.domain_config = domain_config
        self.global_config = domain_config.global_config
        
        ## infra
        self.infra_manager = InfraManager(self.global_config)
        if self.infra_manager.keep() == False:
            raise RuntimeError("Failed to keep infrastructure of havenask")
        
        ## cluster
        self.swift_cluster = SwiftCluster(HapeCommon.SWIFT_KEY, self.domain_config)
        self.suez_cluster = SuezCluster(HapeCommon.HAVENASK_KEY, self.domain_config)
        self.swift_tool_wrapper = self.swift_cluster.swift_tool_wrapper

        self.bs_cluster = BsCluster(HapeCommon.BS_KEY, self.domain_config)

        self.cluster_dict = {
            HapeCommon.SWIFT_KEY: self.swift_cluster,
            HapeCommon.BS_KEY: self.bs_cluster,
            HapeCommon.HAVENASK_KEY: self.suez_cluster,
        }

    def start(self, key, allow_restart=False, only_admin=False):
        start_havenask = (key == HapeCommon.HAVENASK_KEY)
        if start_havenask and (not self.keep_swift_master()):
            return False
            
        cluster = self.cluster_dict[key]
        succ = cluster.start(allow_restart=allow_restart, only_admin=only_admin)
        if not succ:
            Logger.error("Failed to start {} cluster".format(key))
            return False
        
        if start_havenask and not self.keep_bs_master():
            return False

        return True
    
    def get_cluster_status(self, key, detail=False, table=None):
        cluster_status = self.cluster_dict[key].get_status(table=table)
        if cluster_status == None:
            Logger.error("Failed to get {} cluster status".format(key))
        else:
            print(json.dumps(cluster_status, indent=4))
    
    def keep_swift_master(self):
        if not self.swift_cluster.is_ready():
            Logger.info("Swift admin is not ready, begin to start default swift cluster")
            succ = self.swift_cluster.start()
            if not succ:
                Logger.error("Failed to prepare swift for havenask")
                return False
        return True
    
    def keep_bs_master(self):
        if not self.bs_cluster.is_ready():
            Logger.info("Bs admin is not ready, begin to start default bs cluster")
            succ = self.bs_cluster.start()
            if not succ:
                Logger.error("Failed to prepare bs for havenask")
                return False
            return True

    def stop(self, key, is_delete=False):
        if is_delete and key == HapeCommon.HAVENASK_KEY:
            try:
                catalog_manager = self.suez_cluster.get_catalog_manager()
                if catalog_manager != None:
                    table_infos = catalog_manager.list_table()
                    for table in table_infos:
                        Logger.info("Delete swift topic {}".format(table))
                        table_infos = catalog_manager.list_table()
                        self.swift_tool_wrapper.delete_topic(table)
            except:
                Logger.warning("Failed to delete swift topic")


        cluster = self.cluster_dict[key]
        cluster.stop(is_delete=is_delete)

        if key == HapeCommon.HAVENASK_KEY:
            self.bs_cluster.stop(is_delete=is_delete)

        return True
    

    def create_table(self, table, partition, schema, full_data_path):

        is_direct_table = (full_data_path == None)
        build_type = "DIRECT" if is_direct_table else "OFFLINE"

        catalog_manager = self.suez_cluster.catalog_manager
        if catalog_manager == None or not catalog_manager.is_ready():
            Logger.error("Suez or swift admin is not ready, cannot get suez catalog service")
            return False

        if table in catalog_manager.list_table_names():
            Logger.error("table {} already created".format(table))
            return False

        succ = catalog_manager.create_table(table, partition, schema, full_data_path, self.global_config.get_service_appmaster_zk_address(HapeCommon.BS_KEY))
        if not succ:
            return False

        Logger.info("start to create swift topic {}".format(table))
        swift_tool_wrapper = self.swift_tool_wrapper
        is_succ = swift_tool_wrapper.add_topic(table, partition)
        if not is_succ:
            Logger.error("Add swift topic failed")
            return False
        Logger.info("Succeed to create swift topic {}".format(table))

        if is_direct_table:
            Logger.info("{} type table need to be checked ready".format(build_type))
            Logger.debug("Table request is already sent, you can wait or use gs subcommand to check table status")

            def check_table_ready():
                tables = self.suez_cluster.get_ready_tables()
                succ = table in tables
                if not succ:
                    Logger.info("Table not ready in all qrs or searchers by now")
                    Logger.info("If it takes long, you can execute `gs havenask` to find the reason")
                return succ
                    

            msg = "Table {} ready".format(table)
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
            
            
    def delete_table(self, table, keeptopic=False):
        catalog_manager = self.suez_cluster.get_catalog_manager()
        if catalog_manager == None:
            return False


        all_catalog_tables = catalog_manager.list_table_names()
        if table not in all_catalog_tables:
            Logger.error("table {} not exists".format(table))
            return False

        catalog_manager.delete_table(table)

        if not keeptopic:
            self.swift_tool_wrapper.delete_topic(table)

        if len(all_catalog_tables) == 1:
            Logger.info("Delete last table, will not check ready")
            return True
        else:
            Logger.info("Succeed to delete table")
            Logger.info("Before continue to read & write tables, please confirm havenask cluster ready by using gs havenask subcommand".format(table))
            return True
        
        
    def get_table_status(self, specify_table_name=None, detail = False):
        catalog_manager  = self.suez_cluster.get_catalog_manager()
        if catalog_manager != None:
            table_infos = catalog_manager.list_table()
            ready_tables = self.suez_cluster.get_ready_tables()
            result = {}
            for table_name in table_infos:
                if specify_table_name != None and specify_table_name != table_name:
                    continue

                table_info = table_infos[table_name] if detail else {}
                if table_name in ready_tables:
                    table_info["status"] = "READY"
                else:
                    table_info["status"] = "NOT_READY"
                result[table_name] = table_info

            print(json.dumps(result, indent=4))
            return result

        else:
            Logger.error("Failed to get table status, Suez admin maybe not ready")
            return None
        
        
    def update_default_cluster(self, hippo_config, role_type):
        suez_cluster_manager = self.suez_cluster.get_suez_cluster_manager()
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
        suez_cluster_manager = self.suez_cluster.get_suez_cluster_manager()
        hippo_config = suez_cluster_manager.get_default_hippo_config(role_type)
        return hippo_config
    
    
    def update_table_schema(self, table, partition, schema, full_data_path):
        is_direct_table = (full_data_path == None)
        if is_direct_table:
            Logger.error("Only support update table structure for offline mode, data_full_path should be specified")
            return False
        catalog_manager = self.suez_cluster.get_catalog_manager()
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
    
    def create_or_update_load_strategy(self, table, online_index_config):
        catalog_manager = self.suez_cluster.get_catalog_manager()
        all_load_strategies = catalog_manager.list_load_strategy()
        if table not in all_load_strategies:
            Logger.debug("load strategy for table {} not exist, will create".format(table))
            return catalog_manager.create_load_strategy(table, online_index_config)
        else:
            return self.update_load_strategy(table, online_index_config)
        
    def update_load_strategy(self, table, online_index_config):
        catalog_manager = self.suez_cluster.get_catalog_manager()
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
    
    def update_offline_table(self, table, partition, schema, full_data_path, swift_start_timestamp):
        Logger.info("Begin to update offline table")
        is_direct_table = (full_data_path == None)
        if is_direct_table:
            Logger.error("Only support update table structure for offline mode, data_full_path should be specified")
            return False
        catalog_manager = self.suez_cluster.get_catalog_manager()
        if catalog_manager == None:
            return False
        if table not in catalog_manager.list_table_names():
            Logger.error("Table {} not created, create first".format(table))
            return False

        succ = catalog_manager.update_offline_table(table, partition, schema, full_data_path, swift_start_timestamp, 
                                                    self.domain_config.global_config.get_service_appmaster_zk_address(HapeCommon.BS_KEY))
        if not succ:
            Logger.error("Failed to update offline table")
            return False
        return True
    
    def check_new_generation_ready(self):
        generations = []
        for buildinfo in self.list_build():
            generations.append(buildinfo['generationId'])
        generations = sorted(generations)
        if len(generations) == 1:
            return False
        new_generaion = generations[-1]
        
        status = self.suez_cluster.get_status()
        group = "database"
        for role, processor_status_list in status['sqlClusterInfo'][group].items():
            for processor_status in processor_status_list:
                if processor_status['signature'].find(".{}.".format(new_generaion)) == -1:
                    return False
        return True
    
    def wait_new_generation_ready(self):
        succ = Retry.retry(self.check_new_generation_ready, "new generation ready", limit=100)
        return succ
    
    def list_build(self):
        catalog_manager = self.suez_cluster.get_catalog_manager()
        buildinfos = catalog_manager.list_build()
        return buildinfos

    def drop_build(self, name, generation_id):
        catalog_manager = self.suez_cluster.get_catalog_manager()
        try:
            catalog_manager.drop_build(name, generation_id)
        except:
            raise RuntimeError("Failed to drop build, table[{}], generation_id[{}]".format(name, generation_id))

