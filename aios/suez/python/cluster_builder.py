import json
from google.protobuf.json_format import MessageToJson
from aios.suez.admin.ClusterService_pb2 import *
from aios.suez.python.base_builder import BaseBuilder


class ClusterConfigBuilder(BaseBuilder):
    def __init__(self):
        super(ClusterConfigBuilder, self).__init__(ClusterConfig())

    def set_type(self, cluster_type):
        self.msg.type = cluster_type
        return self

    def set_config_str(self, config_str):
        self.msg.configStr = config_str
        return self


class ClusterBuilder(BaseBuilder):
    def __init__(self):
        super(ClusterBuilder, self).__init__(Cluster())

    def set_cluster_name(self, cluster_name):
        self.msg.clusterName = cluster_name
        return self

    def set_version(self, version):
        self.msg.version = version
        return self

    def set_config(self, config):
        if isinstance(config, ClusterConfig):
            self.msg.config.CopyFrom(config)
        else:
            raise ValueError("Invalid ClusterConfig instance")
        return self


class ClusterDeploymentConfigBuilder(BaseBuilder):
    def __init__(self):
        super(ClusterDeploymentConfigBuilder, self).__init__(ClusterDeploymentConfig())

    def set_config_root(self, config_root):
        self.msg.configRoot = config_root
        return self


class ClusterDeploymentBuilder(BaseBuilder):
    def __init__(self):
        super(ClusterDeploymentBuilder, self).__init__(ClusterDeployment())

    def set_deployment_name(self, name):
        self.msg.deploymentName = name

    def add_cluster(self, cluster):
        if isinstance(cluster, Cluster):
            new_cluster = self.msg.clusters.add()
            new_cluster.CopyFrom(cluster)
        else:
            raise ValueError("Invalid Cluster instance")
        return self

    def set_clusters(self, clusters):
        for cluster in clusters:
            self.add_cluster(cluster)
        return self

    def set_config(self, config):
        if isinstance(config, ClusterDeploymentConfig):
            self.msg.config.CopyFrom(config)
        else:
            raise ValueError("Invalid ClusterDeploymentConfig instance")
        return self


class TableGroupBindingBuilder(BaseBuilder):
    def __init__(self):
        super(TableGroupBindingBuilder, self).__init__(DatabaseDeployment.TableGroupBinding())

    def set_table_group_name(self, tg_name):
        self.msg.tableGroupName = tg_name
        return self

    def set_cluster_name(self, cluster_name):
        self.msg.clusterName = cluster_name
        return self


class DatabaseDeploymentBuilder(BaseBuilder):
    def __init__(self):
        super(DatabaseDeploymentBuilder, self).__init__(DatabaseDeployment())

    def set_catalog_name(self, catalog_name):
        self.msg.catalogName = catalog_name
        return self

    def set_database_name(self, database_name):
        self.msg.databaseName = database_name
        return self

    def set_deployment_name(self, deployment_name):
        self.msg.deploymentName = deployment_name
        return self

    def add_binding(self, binding):
        if isinstance(binding, DatabaseDeployment.TableGroupBinding):
            new_binding = self.msg.bindings.add()
            new_binding.CopyFrom(binding)
        else:
            raise ValueError("Invalid TableGroupBinding instance")
        return self

    def add_bindings(self, bindings):
        for binding in bindings:
            self.add_binding(binding)
        return self
