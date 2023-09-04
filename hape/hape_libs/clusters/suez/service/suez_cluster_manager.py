import sys
import os
import requests
import json
from aios.suez.python.cluster_builder import *
from aios.suez.python.catalog_builder import *
from aios.suez.admin.ClusterService_pb2 import ClusterConfig as SuezClusterConfig

from .suez_cluster_service import SuezClusterService
from hape_libs.container import *
from hape_libs.utils.logger import Logger
from hape_libs.utils.template import TemplateRender
from hape_libs.common import *
from hape_libs.config import *
import traceback 

class SuezClusterManager(object):
    def __init__(self, http_address, swift_zk, suez_zk, cluster_config):
        self._swift_zfs_path = swift_zk
        self._suez_zfs_path = suez_zk
        self._cluster_service = SuezClusterService(http_address)
        self._cluster_config = cluster_config
        self._container_service = RemoteDockerContainerService(cluster_config)
        
    def _parse_cluster_hippo_config(self):
        Logger.debug("Parse suez hippo plan in template")
        try:
            data = self._cluster_config.template_config.get_local_template_content("havenask/hippo/qrs_hippo.json")
            qrs_hippo_config = json.loads(data)
        except Exception as e:
            Logger.error(traceback.format_exc())
            raise ValueError("Fail to parse qrs hippo config template, not exist or has wrong format")
        
        try:
            data = self._cluster_config.template_config.get_local_template_content("havenask/hippo/searcher_hippo.json")
            searcher_hippo_config = json.loads(data)
        except Exception as e:
            Logger.error(traceback.format_exc())
            raise ValueError("Fail to parse searcher hippo config template, not exist or has wrong format")
        return qrs_hippo_config, searcher_hippo_config
    
    
    def create_or_update_default_clusters(self):
        Logger.info("Create default cluster")
        
        qrs_hippo_config, searcher_hippo_config = self._parse_cluster_hippo_config()
        store_root = self._cluster_config.get_suez_cluster_store_root()
        
        cluster_deployment = ClusterDeploymentBuilder()
        cluster_deployment.set_deployment_name(HapeCommon.DEFAULT_DEPLOYMENT)
        
        cluster = ClusterBuilder()
        cluster.set_cluster_name(HapeCommon.DEFAULT_SEARCHER_CLUSTERNAME)
        cluster_config = ClusterConfigBuilder()
        cluster.set_config(cluster_config
                           .set_type(SuezClusterConfig.ClusterType.CT_SEARCHER)
                           .set_config_str(json.dumps({"hippo_config": json.dumps(searcher_hippo_config)}))
                           .build())
        
        cluster_deployment.add_cluster(cluster.build())
        cluster_deployment.set_config(ClusterDeploymentConfigBuilder().set_config_root(store_root).build())
        
        
        biz_config = self._cluster_config.get_suez_cluster_store_root()
        if not biz_config.startswith("hdfs://"):
            biz_config = "LOCAL:/" + biz_config
        
        cluster = ClusterBuilder()
        cluster.set_cluster_name("qrs")
        cluster_config = ClusterConfigBuilder()
        cluster.set_config(cluster_config
                           .set_type(SuezClusterConfig.ClusterType.CT_QRS)
                           .set_config_str(json.dumps({
                               "hippo_config": json.dumps(qrs_hippo_config)
                           }))
                           .build())

        cluster_deployment.add_cluster(cluster.build())
        cluster_deployment.set_config(ClusterDeploymentConfigBuilder().set_config_root(biz_config).build())
        self._cluster_service.create_cluster(cluster_deployment.to_json())
        
        
        database_deployment = DatabaseDeploymentBuilder() \
            .set_catalog_name(HapeCommon.DEFAULT_CATALOG) \
            .set_database_name(HapeCommon.DEFAULT_DATABASE) \
            .set_deployment_name(HapeCommon.DEFAULT_DEPLOYMENT)
        
        database_deployment.add_binding(TableGroupBindingBuilder()
                                        .set_table_group_name(HapeCommon.DEFAULT_TABLEGROUP)
                                        .set_cluster_name(HapeCommon.DEFAULT_SEARCHER_CLUSTERNAME)
                                        .build())
        self._cluster_service.deploy_database(database_deployment.to_json())