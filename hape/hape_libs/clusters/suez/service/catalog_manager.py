import sys
import os
from google.protobuf.json_format import ParseDict
import json
import time

from aios.suez.python.catalog_builder import *
from aios.catalog.proto.CatalogEntity_pb2 import *

from .catalog_service import CatalogService
from hape_libs.utils.logger import Logger
from hape_libs.utils.havenask_schema import HavenaskSchema
from hape_libs.common import *
from hape_libs.config import *
import copy
import traceback


class CatalogManager:
    def __init__(self, address, swift_zfs_path, cluster_config): #type: (str, str, ClusterConfig)->None
        self._swift_zk_path = swift_zfs_path
        self._catalog_service = CatalogService(address)
        self._cluster_config = cluster_config
        
        
    def create_default_catalog(self):
        Logger.info("Start to create default catalog")
        catalog = CatalogBuilder().set_catalog_name(HapeCommon.DEFAULT_CATALOG)
        self._catalog_service.create_catalog(catalog_json=catalog.to_json())
        Logger.info("Succeed to create default catalog")
        self.create_default_database()
    
    def create_default_database(self):
        Logger.info("Start to create default database")
        
        storeRoot = self._cluster_config.get_db_store_root()
        if not storeRoot.startswith("hdfs://"):
            storeRoot = "LOCAL:/" + storeRoot
        
        database = DatabaseBuilder().set_catalog_name(HapeCommon.DEFAULT_CATALOG)\
                                    .set_database_name(HapeCommon.DEFAULT_DATABASE)\
                                    .set_database_config(DatabaseConfigBuilder().set_store_root(storeRoot).build())\
                                    .add_table_group(
                                        TableGroupBuilder().set_table_group_name(HapeCommon.DEFAULT_TABLEGROUP)\
                                                           .set_database_name(HapeCommon.DEFAULT_DATABASE)\
                                                           .set_catalog_name(HapeCommon.DEFAULT_CATALOG).build()
                                   )
        self._catalog_service.create_database(HapeCommon.DEFAULT_CATALOG, database.to_json())
        Logger.info("Succeed to create default database")
    
    def create_table(self, name, partition_count, schema_path, data_full_path, bs_zfs_path = None):
        catalogname = HapeCommon.DEFAULT_CATALOG
        databasename = HapeCommon.DEFAULT_DATABASE
        
        is_direct_table = (data_full_path == None)
        build_type = BuildType.DIRECT if is_direct_table else BuildType.OFFLINE
        Logger.info("Start to create table {}, type: {}".format(name, build_type))
        try:
            shard_field, schema = self._parse_schema(schema_path)
            shard_info = TableStructureConfig.ShardInfo()
            shard_info.shard_fields.extend([shard_field])
            shard_info.shard_func = "HASH"
            shard_info.shard_count = int(partition_count)
            
            columns = [ParseDict(column, TableStructure.Column()) for column in schema["columns"]]
            indexes = [ParseDict(index, TableStructure.Index()) for index in schema["indexes"]]
            
            table_structure_config = TableStructureConfigBuilder().set_shard_info(shard_info).set_build_type(build_type).set_table_type(TableType.NORMAL)
            table_structure = TableStructureBuilder().set_table_name(name).set_database_name(databasename).set_catalog_name(catalogname).\
                set_table_structure_config(table_structure_config.build()).set_columns(columns).set_indexes(indexes)
            
            
            swift_data_description = DataSource.DataVersion.DataDescription()
            swift_data_description.src = "swift"
            swift_data_description.swift_root = self._swift_zk_path
            swift_data_description.swift_topic_name = name
            # data_descriptions[0]["swift_start_timestamp"] = str(int(time.time()) * 1000 * 1000 - 60 * 1000 * 1000)
            
            custom_metas = {
                "swift_root": self._swift_zk_path,
                "template_md5": self._cluster_config.template_config.md5
            }
            
            if not is_direct_table:
                ## swift without stoptimestamp must set as second datasource in multi datasources description, otherwise processor.full will not stop
                file_data_description = DataSource.DataVersion.DataDescription()
                file_data_description.src = "file"
                if data_full_path.startswith("/") or data_full_path.find("://")!=-1:
                    pass
                else:
                    data_full_path = os.path.realpath(data_full_path)
                file_data_description.data_path = data_full_path
                file_data_description.swift_topic_name = name
                data_descriptions = [file_data_description, swift_data_description]
                custom_metas["zookeeper_root"] = bs_zfs_path
            else:
                data_descriptions = [swift_data_description]

            partition_config = PartitionConfigBuilder().set_custom_metas(custom_metas)
            data_version = DataSource.DataVersion()
            data_version.data_descriptions.extend(data_descriptions)
            data_source = DataSourceBuilder()
            data_source.set_data_version(data_version)
            partition = PartitionBuilder().set_partition_name(name+"_part").set_table_name(name)\
                                    .set_database_name(databasename).set_catalog_name(catalogname)\
                                    .set_partition_type(PartitionType.TABLE_PARTITION).set_partition_config(partition_config.build()).set_data_source(data_source.build())
                                    
            table = TableBuilder().set_catalog_name(catalogname).set_database_name(databasename)\
                        .set_table_name(name).set_table_structure(table_structure.build())
            table.add_partitions(partition.build())
            self._catalog_service.create_table(catalogname, table.to_json())
            return True
        except Exception as e:
            Logger.error("Failed to create table, detail:{}".format(traceback.format_exc()))
            return False

        
    def delete_table(self, name):
        Logger.debug("delete table:{}".format(name))
        self._catalog_service.delete_table(HapeCommon.DEFAULT_CATALOG, HapeCommon.DEFAULT_DATABASE, name)
        
        
    def _parse_schema(self, schema_path):
        with open(schema_path, "r") as f:
            schema_dict = json.load(f)
            
        Logger.info("Raw schema {}".format(json.dumps(schema_dict, indent=4)))
        ha_schema = HavenaskSchema(schema_dict)
        ha_schema.parse()
        Logger.info("Processed schema {}".format(json.dumps(ha_schema.schema, indent=4)))
        return ha_schema.shard_field, schema_dict
    
    
    def list_table(self):
        names = self._catalog_service.list_table(HapeCommon.DEFAULT_CATALOG, HapeCommon.DEFAULT_DATABASE)
        result = {}
        for name in names:
            table_info = self._catalog_service.get_table(HapeCommon.DEFAULT_CATALOG, HapeCommon.DEFAULT_DATABASE, name)
            result[name] = table_info["tableStructure"]
        return result
    
    def list_table_names(self):
        names = self._catalog_service.list_table(HapeCommon.DEFAULT_CATALOG, HapeCommon.DEFAULT_DATABASE)
        return names
    
        
    def get_build(self, name):
        return self._catalog_service.get_build(HapeCommon.DEFAULT_CATALOG, HapeCommon.DEFAULT_DATABASE, name)