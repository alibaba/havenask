import sys
import os
from google.protobuf.json_format import ParseDict
from hape_libs.common import HapeCommon
import json
import time

from aios.suez.python.catalog_builder import *
from aios.catalog.proto.CatalogEntity_pb2 import *

from .catalog_service import CatalogService
from hape_libs.utils.logger import Logger
from hape_libs.utils.havenask_schema import HavenaskSchema
from hape_libs.config import *
import copy
import traceback


class CatalogManager:
    def __init__(self, address, domain_config): #type: (str, str, DomainConfig)->None
        self._address = address
        self._catalog_service = CatalogService(address)
        self._domain_config = domain_config
        self._swift_zk_path = domain_config.global_config.get_service_appmaster_zk_address(HapeCommon.SWIFT_KEY)
        
    def is_ready(self):
        return self._address != None


    def create_default_catalog(self):
        Logger.info("Start to create default catalog")
        catalog = CatalogBuilder().set_catalog_name(HapeCommon.DEFAULT_CATALOG)
        self._catalog_service.create_catalog(catalog_json=catalog.to_json())
        Logger.info("Succeed to create default catalog")
        self._create_default_database()

    def _create_default_database(self):
        Logger.info("Start to create default database")

        storeRoot = self._domain_config.global_config.havenask.dbStoreRoot
        if not storeRoot.startswith("hdfs://") and not storeRoot.startswith("jfs://"):
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

            table_meta_option = ParseDict({"enable_all_text_field_meta": True}, TableStructureConfig.TableMetaOption())

            table_structure_config = TableStructureConfigBuilder().set_shard_info(shard_info).set_build_type(build_type).set_table_type(TableType.NORMAL).set_table_meta(table_meta_option)
            table_structure = TableStructureBuilder().set_table_name(name).set_database_name(databasename).set_catalog_name(catalogname).\
                set_table_structure_config(table_structure_config.build()).set_columns(columns).set_indexes(indexes)


            swift_data_description = DataSource.DataVersion.DataDescription()
            swift_data_description.src = "swift"
            swift_data_description.swift_root = self._swift_zk_path
            swift_data_description.swift_topic_name = name

            custom_metas = {
                "swift_root": self._swift_zk_path,
                "template_md5": self._domain_config.template_config.md5_signature
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

    def update_offline_table(self, name, partition_count, schema_path, data_full_path, swift_start_timestamp, bs_zfs_path = None):
        if not self.update_partition(name, partition_count, schema_path, data_full_path, swift_start_timestamp, bs_zfs_path):
            return False
        return True

    def update_table_structure(self, name, partition_count, schema_path, data_full_path, swift_start_timestamp, bs_zfs_path = None):
        catalogname = HapeCommon.DEFAULT_CATALOG
        databasename = HapeCommon.DEFAULT_DATABASE
        is_direct_table = (data_full_path == None)
        if is_direct_table:
            Logger.error("Only support update table structure for offline mode, data_full_path should be specified")
            return False
        try:
            shard_field, schema = self._parse_schema(schema_path)
            shard_info = TableStructureConfig.ShardInfo()
            shard_info.shard_fields.extend([shard_field])
            shard_info.shard_func = "HASH"
            shard_info.shard_count = int(partition_count)
            build_type = BuildType.OFFLINE
            columns = [ParseDict(column, TableStructure.Column()) for column in schema["columns"]]
            indexes = [ParseDict(index, TableStructure.Index()) for index in schema["indexes"]]
            table_structure_config = TableStructureConfigBuilder().set_shard_info(shard_info)\
                                                                  .set_build_type(build_type)\
                                                                  .set_table_type(TableType.NORMAL)
            table_structure = TableStructureBuilder().set_table_name(name)\
                                                     .set_database_name(databasename)\
                                                     .set_catalog_name(catalogname)\
                                                     .set_table_structure_config(table_structure_config.build())\
                                                     .set_columns(columns)\
                                                     .set_indexes(indexes)
            self._catalog_service.update_table_structure(catalogname, table_structure.to_json())
            return True
        except Exception as e:
            Logger.error("Failed to update table structure, detail:{}".format(traceback.format_exc()))
            return False

    def update_partition_table_structure(self, table_name):
        catalog_name = HapeCommon.DEFAULT_CATALOG
        database_name = HapeCommon.DEFAULT_DATABASE
        partition_name = table_name + "_part"
        try:
            self._catalog_service.update_partition_table_structure(catalog_name,
                                                                   database_name,
                                                                   table_name,
                                                                   partition_name)
            return True
        except Exception as e:
            Logger.error("Failed to update partition table structure, detail:{}".format(traceback.format_exc()))
            return False

    def update_partition(self, table_name, partition_count, schema_path, data_full_path, swift_start_timestamp, bs_zfs_path = None):
        catalogname = HapeCommon.DEFAULT_CATALOG
        databasename = HapeCommon.DEFAULT_DATABASE
        if data_full_path == None:
            Logger.error("Only support update partition for offline mode, data_full_path should be specified")
            return False
        try:
            # table structure
            shard_field, schema = self._parse_schema(schema_path)
            shard_info = TableStructureConfig.ShardInfo()
            shard_info.shard_fields.extend([shard_field])
            shard_info.shard_func = "HASH"
            shard_info.shard_count = int(partition_count)
            build_type = BuildType.OFFLINE
            columns = [ParseDict(column, TableStructure.Column()) for column in schema["columns"]]
            indexes = [ParseDict(index, TableStructure.Index()) for index in schema["indexes"]]
            table_structure_config = TableStructureConfigBuilder().set_shard_info(shard_info)\
                                                                  .set_build_type(build_type)\
                                                                  .set_table_type(TableType.NORMAL)
            table_structure = TableStructureBuilder().set_table_name(table_name)\
                                                     .set_database_name(databasename)\
                                                     .set_catalog_name(catalogname)\
                                                     .set_table_structure_config(table_structure_config.build())\
                                                     .set_columns(columns)\
                                                     .set_indexes(indexes)
            # data source
            swift_data_description = DataSource.DataVersion.DataDescription()
            swift_data_description.src = "swift"
            swift_data_description.swift_root = self._swift_zk_path
            swift_data_description.swift_topic_name = table_name
            swift_data_description.swift_start_timestamp = str(swift_start_timestamp)
            file_data_description = DataSource.DataVersion.DataDescription()
            file_data_description.src = "file"
            if data_full_path.startswith("/") or data_full_path.find("://")!=-1:
                pass
            else:
                data_full_path = os.path.realpath(data_full_path)
            file_data_description.data_path = data_full_path
            file_data_description.swift_topic_name = table_name
            data_descriptions = [file_data_description, swift_data_description]
            data_version = DataSource.DataVersion()
            data_version.data_descriptions.extend(data_descriptions)
            data_source = DataSourceBuilder().set_data_version(data_version)
            # partition config
            custom_metas = {
                "swift_root": self._swift_zk_path,
                "template_md5": self._domain_config.template_config.md5_signature,
                "zookeeper_root": bs_zfs_path
            }
            partition_config = PartitionConfigBuilder().set_custom_metas(custom_metas)
        except Exception as e:
            Logger.error("Failed to construct update partition params, detail:{}".format(traceback.format_exc()))
            return False
        return self.update_partition_with_builder(table_name,
                                                  partition_config=partition_config,
                                                  data_source=data_source,
                                                  table_structure=table_structure)


    def update_partition_with_builder(self, table_name, partition_config=None,
                                      data_source=None, table_structure=None):
        if partition_config is None and data_source is None and table_structure is None:
            Logger.error("Failed to update partition, no diff")
            return False
        catalog_name = HapeCommon.DEFAULT_CATALOG
        database_name = HapeCommon.DEFAULT_DATABASE
        partition_name = table_name + "_part"
        try:
            partition = PartitionBuilder().set_partition_name(partition_name)\
                                          .set_table_name(table_name)\
                                          .set_database_name(database_name)\
                                          .set_catalog_name(catalog_name)\
                                          .set_partition_type(PartitionType.TABLE_PARTITION)
            if partition_config:
                partition.set_partition_config(partition_config.build())
            if data_source:
                partition.set_data_source(data_source.build())
            if partition_name:
                partition.set_table_structure(table_structure.build())
            self._catalog_service.update_partition(catalog_name, partition.to_json())
            return True
        except Exception as e:
            Logger.error("Failed to update partition, detail:{}".format(traceback.format_exc()))
            return False

    def delete_table(self, name):
        Logger.debug("delete table:{}".format(name))
        self._catalog_service.delete_table(HapeCommon.DEFAULT_CATALOG, HapeCommon.DEFAULT_DATABASE, name)

    def update_load_strategy(self, table_name, online_index_config):
        Logger.debug("update load strategy with online index config:{}".format(online_index_config))
        try:
            load_strategy_config = LoadStrategyConfigBuilder()\
                .set_online_index_config(online_index_config)\
                .set_load_mode(LoadStrategyConfig.USER_DEFINED)
            load_strategy = LoadStrategyBuilder().set_catalog_name(HapeCommon.DEFAULT_CATALOG)\
                                                 .set_database_name(HapeCommon.DEFAULT_DATABASE)\
                                                 .set_table_group_name(HapeCommon.DEFAULT_TABLEGROUP)\
                                                 .set_table_name(table_name)\
                                                 .set_load_strategy_config(load_strategy_config.build())
            self._catalog_service.update_load_strategy(HapeCommon.DEFAULT_CATALOG,
                                                       load_strategy.to_json())
            return True
        except Exception as e:
            Logger.error("Failed to update load strategy, detail:{}".format(traceback.format_exc()))
            return False

    def create_load_strategy(self, table_name, online_index_config):
        Logger.debug("create load strategy with online index config:{}".format(online_index_config))
        try:
            load_strategy_config = LoadStrategyConfigBuilder()\
                .set_online_index_config(online_index_config)\
                .set_load_mode(LoadStrategyConfig.USER_DEFINED)
            load_strategy = LoadStrategyBuilder().set_catalog_name(HapeCommon.DEFAULT_CATALOG)\
                                                 .set_database_name(HapeCommon.DEFAULT_DATABASE)\
                                                 .set_table_group_name(HapeCommon.DEFAULT_TABLEGROUP)\
                                                 .set_table_name(table_name)\
                                                 .set_load_strategy_config(load_strategy_config.build())
            self._catalog_service.create_load_strategy(HapeCommon.DEFAULT_CATALOG,
                                                       load_strategy.to_json())
            return True
        except Exception as e:
            Logger.error("Failed to create load strategy, detail:{}".format(traceback.format_exc()))
            return False

    def list_load_strategy(self):
        return self._catalog_service.list_load_strategy(HapeCommon.DEFAULT_CATALOG,
                                                          HapeCommon.DEFAULT_DATABASE,
                                                          HapeCommon.DEFAULT_TABLEGROUP)

    def _parse_schema(self, schema_path):
        with open(schema_path, "r") as f:
            schema_dict = json.load(f)

        Logger.info("Raw schema {}".format(json.dumps(schema_dict, indent=4)))
        ha_schema = HavenaskSchema(schema_dict)
        ha_schema.parse(enable_parse_types=False)
        Logger.info("Processed schema {}".format(json.dumps(ha_schema.schema, indent=4)))
        return ha_schema.shard_field, schema_dict


    def list_table(self):
        names = self._catalog_service.list_table(HapeCommon.DEFAULT_CATALOG, HapeCommon.DEFAULT_DATABASE)
        result = {}
        for name in names:
            table_info = self._catalog_service.get_table(HapeCommon.DEFAULT_CATALOG, HapeCommon.DEFAULT_DATABASE, name)
            result[name] = table_info["tableStructure"]
        return result
    
    
    def get_table_shard_count(self, table_name):
        table_info = self._catalog_service.get_table(HapeCommon.DEFAULT_CATALOG, HapeCommon.DEFAULT_DATABASE, table_name)
        parts = table_info.get("partitions", [])
        if len(parts) != 1:
            raise RuntimeError("Failed to get partitions of table {}".format(table_name))
        
        count = parts[0].get('tableStructure', {}).get('tableStructureConfig', {}).get('shardInfo', {}).get('shardCount', -1)
        if count < 1:
            raise RuntimeError("Failed to get shard count of table {}".format(table_name))
        return count

    def list_table_names(self):
        names = self._catalog_service.list_table(HapeCommon.DEFAULT_CATALOG, HapeCommon.DEFAULT_DATABASE)
        return names


    def get_build(self, name):
        return self._catalog_service.get_build(HapeCommon.DEFAULT_CATALOG, HapeCommon.DEFAULT_DATABASE, name)
    
    def list_build(self):
        return self._catalog_service.list_build(HapeCommon.DEFAULT_CATALOG)

    def drop_build(self, name, generation_id):
        partition_name = name + "_part"
        return self._catalog_service.drop_build(HapeCommon.DEFAULT_CATALOG,
                                                HapeCommon.DEFAULT_DATABASE,
                                                name,
                                                partition_name,
                                                generation_id)
    
    def update_table_schema(self, name, partition_count, schema_path, data_full_path, bs_zfs_path = None):
        if not self.update_partition(name, partition_count, schema_path, data_full_path, bs_zfs_path):
            return False
        return True
