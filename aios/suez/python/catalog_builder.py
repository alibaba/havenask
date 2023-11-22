import json
from google.protobuf.json_format import MessageToJson, Parse
from aios.catalog.proto.CatalogEntity_pb2 import *
from aios.suez.python.base_builder import BaseBuilder


class TableTypeBuilder(BaseBuilder):
    def __init__(self):
        self.msg = TableType()

    def set_code(self, value):
        self.msg.code = value
        return self


class BuildTypeBuilder(BaseBuilder):
    def __init__(self):
        self.msg = BuildType()

    def set_code(self, value):
        self.msg.code = value
        return self


class TableStructureConfigBuilder(BaseBuilder):
    def __init__(self):
        self.msg = TableStructureConfig()

    def set_shard_info(self, value):
        if not isinstance(value, TableStructureConfig.ShardInfo):
            raise ValueError('Invalid type for field shard_info, expected ShardInfo')

        self.msg.shard_info.CopyFrom(value)
        return self

    def set_table_type(self, value):
        self.msg.table_type = value
        return self

    def set_comment(self, value):
        self.msg.comment = value
        return self

    def set_ttl_option(self, value):
        if not isinstance(value, TtlOption):
            raise ValueError('Invalid type for field ttl_option, expected TtlOption')

        self.msg.ttl_option.CopyFrom(value)
        return self
    
    def set_table_meta(self, value):
        if not isinstance(value, TableStructureConfig.TableMetaOption):
            raise ValueError('Invalid type for field table_meta_option, expected TtlOption')

        self.msg.table_meta_option.CopyFrom(value)
        return self

    def set_sort_option(self, value):
        if not isinstance(value, SortOption):
            raise ValueError('Invalid type for field sort_option, expected SortOption')

        self.msg.sort_option.CopyFrom(value)
        return self

    def set_build_type(self, value):
        self.msg.build_type = value
        return self


class ColumnBuilder(BaseBuilder):
    def __init__(self):
        self.msg = TableStructure.Column()


class IndexBuilder(BaseBuilder):
    def __init__(self):
        self.msg = TableStructure.Index()


class TableStructureBuilder(BaseBuilder):
    def __init__(self):
        self.msg = TableStructure()

    def set_version(self, value):
        self.msg.version = value
        return self

    def set_status(self, value):
        if not isinstance(value, EntityStatus):
            raise ValueError('Invalid type for field status, expected EntityStatus')

        self.msg.status.CopyFrom(value)
        return self

    def set_table_name(self, value):
        self.msg.table_name = value
        return self

    def set_database_name(self, value):
        self.msg.database_name = value
        return self

    def set_catalog_name(self, value):
        self.msg.catalog_name = value
        return self

    def add_column(self, value):
        if not isinstance(value, TableStructure.Column):
            raise ValueError('Invalid type for field columns, expected Column')

        column = self.msg.columns.add()
        column.CopyFrom(value)
        return self

    def set_columns(self, values):
        for value in values:
            self.add_column(value)
        return self

    def add_index(self, value):
        if not isinstance(value, TableStructure.Index):
            raise ValueError('Invalid type for field indexes, expected Index')

        index = self.msg.indexes.add()
        index.CopyFrom(value)
        return self

    def set_indexes(self, values):
        for value in values:
            self.add_index(value)
        return self

    def set_table_structure_config(self, value):
        if not isinstance(value, TableStructureConfig):
            raise ValueError('Invalid type for field table_structure_config, expected TableStructureConfig')

        self.msg.table_structure_config.CopyFrom(value)
        return self

    def set_operation_meta(self, value):
        if not isinstance(value, OperationMeta):
            raise ValueError('Invalid type for field operation_meta, expected OperationMeta')

        self.msg.operation_meta.CopyFrom(value)
        return self


class PartitionConfigBuilder(BaseBuilder):
    def __init__(self):
        self.msg = PartitionConfig()

    def set_version(self, value):
        self.msg.version = value
        return self

    def set_status(self, value):
        if not isinstance(value, EntityStatus):
            raise ValueError('Invalid type for field status, expected EntityStatus')

        self.msg.status.CopyFrom(value)
        return self

    def set_custom_metas(self, value):
        self.msg.custom_metas.update(value)
        return self


class DataSourceBuilder(BaseBuilder):
    def __init__(self):
        self.msg = DataSource()

    def set_data_version(self, value):
        if not isinstance(value, DataSource.DataVersion):
            raise ValueError('Invalid type for field status, expected EntityStatus')

        self.msg.data_version.CopyFrom(value)
        return self


class PartitionBuilder(BaseBuilder):
    def __init__(self):
        self.msg = Partition()

    def set_version(self, value):
        self.msg.version = value
        return self

    def set_status(self, value):
        if not isinstance(value, EntityStatus):
            raise ValueError('Invalid type for field status, expected EntityStatus')

        self.msg.status.CopyFrom(value)
        return self

    def set_partition_name(self, value):
        self.msg.partition_name = value
        return self

    def set_table_name(self, value):
        self.msg.table_name = value
        return self

    def set_database_name(self, value):
        self.msg.database_name = value
        return self

    def set_catalog_name(self, value):
        self.msg.catalog_name = value
        return self

    def set_partition_type(self, value):
        self.msg.partition_type = value
        return self

    def set_partition_config(self, value):
        if not isinstance(value, PartitionConfig):
            raise ValueError('Invalid type for field partition_config, expected PartitionConfig')

        self.msg.partition_config.CopyFrom(value)
        return self

    def set_data_source(self, value):
        if not isinstance(value, DataSource):
            raise ValueError('Invalid type for field data_source, expected DataSource')

        self.msg.data_source.CopyFrom(value)
        return self

    def set_table_structure(self, value):
        if not isinstance(value, TableStructure):
            raise ValueError('Invalid type for field table_structure, expected TableStructure')

        self.msg.table_structure.CopyFrom(value)
        return self

    def set_operation_meta(self, value):
        if not isinstance(value, OperationMeta):
            raise ValueError('Invalid type for field operation_meta, expected OperationMeta')

        self.msg.operation_meta.CopyFrom(value)
        return self


class TableConfigBuilder(BaseBuilder):
    def __init__(self):
        self.msg = TableConfig()

    def set_version(self, value):
        self.msg.version = value
        return self

    def set_status(self, value):
        if not isinstance(value, EntityStatus):
            raise ValueError('Invalid type for field status, expected EntityStatus')

        self.msg.status.CopyFrom(value)
        return self

    def set_partition_ttl(self, value):
        self.msg.partition_ttl = value
        return self

    def set_offline_app_name(self, value):
        self.msg.offline_app_name = value
        return self


class TableBuilder(BaseBuilder):
    def __init__(self):
        self.msg = Table()

    def set_version(self, value):
        self.msg.version = value
        return self

    def set_status(self, value):
        if not isinstance(value, EntityStatus):
            raise ValueError('Invalid type for field status, expected EntityStatus')

        self.msg.status.CopyFrom(value)
        return self

    def set_table_name(self, value):
        self.msg.table_name = value
        return self

    def set_database_name(self, value):
        self.msg.database_name = value
        return self

    def set_catalog_name(self, value):
        self.msg.catalog_name = value
        return self

    def set_table_config(self, value):
        if not isinstance(value, TableConfig):
            raise ValueError('Invalid type for field table_config, expected TableConfig')

        self.msg.table_config.CopyFrom(value)
        return self

    def set_table_structure(self, value):
        if not isinstance(value, TableStructure):
            raise ValueError('Invalid type for field table_structure, expected TableStructure')

        self.msg.table_structure.CopyFrom(value)
        return self

    def add_partitions(self, value):
        if not isinstance(value, Partition):
            raise ValueError('Invalid type for field partitions, expected Partition')

        partition = self.msg.partitions.add()
        partition.CopyFrom(value)
        return self

    def set_partitions(self, values):
        for value in values:
            self.add_partition(value)
        return self

    def set_operation_meta(self, value):
        if not isinstance(value, OperationMeta):
            raise ValueError('Invalid type for field operation_meta, expected OperationMeta')

        self.msg.operation_meta.CopyFrom(value)
        return self


class FunctionConfigBuilder(BaseBuilder):
    def __init__(self):
        self.msg = FunctionConfig()

    def set_version(self, value):
        self.msg.version = value
        return self

    def set_function_type(self, value):
        self.msg.function_type = value
        return self


class FunctionBuilder(BaseBuilder):
    def __init__(self):
        self.msg = Function()

    def set_version(self, value):
        self.msg.version = value
        return self

    def set_status(self, value):
        if not isinstance(value, EntityStatus):
            raise ValueError('Invalid type for field status, expected EntityStatus')

        self.msg.status.CopyFrom(value)
        return self

    def set_function_name(self, value):
        self.msg.function_name = value
        return self

    def set_database_name(self, value):
        self.msg.database_name = value
        return self

    def set_catalog_name(self, value):
        self.msg.catalog_name = value
        return self

    def set_function_config(self, value):
        if not isinstance(value, FunctionConfig):
            raise ValueError('Invalid type for field function_config, expected FunctionConfig')

        self.msg.function_config.CopyFrom(value)
        return self

    def set_operation_meta(self, value):
        if not isinstance(value, OperationMeta):
            raise ValueError('Invalid type for field operation_meta, expected OperationMeta')

        self.msg.operation_meta.CopyFrom(value)
        return self


class DatabaseConfigBuilder(BaseBuilder):
    def __init__(self):
        self.msg = DatabaseConfig()

    def set_version(self, value):
        self.msg.version = value
        return self

    def set_status(self, value):
        if not isinstance(value, EntityStatus):
            raise ValueError('Invalid type for field status, expected EntityStatus')

        self.msg.status.CopyFrom(value)
        return self

    def set_configs(self, value):
        self.msg.configs = value
        return self

    def set_custom_meta(self, value):
        self.msg.custom_meta.update(value)
        return self

    def set_access_tokens(self, value):
        self.msg.access_tokens = value
        return self

    def set_store_root(self, value):
        self.msg.store_root = value
        return self


class DatabaseBuilder(BaseBuilder):
    def __init__(self):
        self.msg = Database()

    def set_version(self, value):
        self.msg.version = value
        return self

    def set_status(self, value):
        if not isinstance(value, EntityStatus):
            raise ValueError('Invalid type for field status, expected EntityStatus')

        self.msg.status.CopyFrom(value)
        return self

    def set_database_name(self, value):
        self.msg.database_name = value
        return self

    def set_catalog_name(self, value):
        self.msg.catalog_name = value
        return self

    def set_database_config(self, value):
        if not isinstance(value, DatabaseConfig):
            raise ValueError('Invalid type for field database_config, expected DatabaseConfig')

        self.msg.database_config.CopyFrom(value)
        return self

    def add_table(self, value):
        if not isinstance(value, Table):
            raise ValueError('Invalid type for field tables, expected Table')

        table = self.msg.tables.add()
        table.CopyFrom(value)
        return self

    def set_tables(self, values):
        for value in values:
            self.add_table(value)

        return self

    def add_table_group(self, value):
        if not isinstance(value, TableGroup):
            raise ValueError('Invalid type for field table_groups, expected TableGroup')

        table_group = self.msg.table_groups.add()
        table_group.CopyFrom(value)
        return self

    def set_table_groups(self, values):
        for value in values:
            self.add_table_group(value)
        return self

    def add_function(self, value):
        if not isinstance(value, Function):
            raise ValueError('Invalid type for field functions, expected Function')

        function = self.msg.functions.add()
        function.CopyFrom(value)
        return self

    def add_functions(self, values):
        for value in values:
            self.add_function(value)
        return self

    def set_operation_meta(self, value):
        if not isinstance(value, OperationMeta):
            raise ValueError('Invalid type for field operation_meta, expected OperationMeta')

        self.msg.operation_meta.CopyFrom(value)
        return self


class LoadStrategyConfigBuilder(BaseBuilder):
    def __init__(self):
        self.msg = LoadStrategyConfig()

    def set_version(self, value):
        self.msg.version = value
        return self

    def set_status(self, value):
        if not isinstance(value, EntityStatus):
            raise ValueError('Invalid type for field status, expected EntityStatus')

        self.msg.status = value
        return self

    def set_load_mode(self, value):
        self.msg.load_mode = value
        return self

    def set_online_index_config(self, value):
        self.msg.online_index_config = value
        return self


class LoadStrategyBuilder(BaseBuilder):
    def __init__(self):
        self.msg = LoadStrategy()

    def set_version(self, value):
        self.msg.version = value
        return self

    def set_status(self, value):
        if not isinstance(value, EntityStatus):
            raise ValueError('Invalid type for field status, expected EntityStatus')

        self.msg.status.CopyFrom(value)
        return self

    def set_table_group_name(self, value):
        self.msg.table_group_name = value
        return self

    def set_catalog_name(self, value):
        self.msg.catalog_name = value
        return self

    def set_table_name(self, value):
        self.msg.table_name = value
        return self

    def set_database_name(self, value):
        self.msg.database_name = value
        return self

    def set_load_strategy_config(self, value):
        if not isinstance(value, LoadStrategyConfig):
            raise ValueError('Invalid type for field load_strategy_config, expected LoadStrategyConfig')

        self.msg.load_strategy_config.CopyFrom(value)
        return self

    def set_operation_meta(self, value):
        if not isinstance(value, OperationMeta):
            raise ValueError('Invalid type for field operation_meta, expected OperationMeta')

        self.msg.operation_meta.CopyFrom(value)
        return self


class TableGroupConfigBuilder(BaseBuilder):
    def __init__(self):
        self.msg = TableGroupConfig()

    def set_version(self, value):
        self.msg.version = value
        return self

    def set_status(self, value):
        if not isinstance(value, EntityStatus):
            raise ValueError('Invalid type for field status, expected EntityStatus')

        self.msg.status.CopyFrom(value)
        return self

    def set_default_load_strategy_config(self, value):
        if not isinstance(value, LoadStrategyConfig):
            raise ValueError('Invalid type for field default_load_strategy_config, expected LoadStrategyConfig')

        self.msg.default_load_strategy_config.CopyFrom(value)
        return self


class TableGroupBuilder(BaseBuilder):
    def __init__(self):
        self.msg = TableGroup()

    def set_version(self, value):
        self.msg.version = value
        return self

    def set_status(self, value):
        if not isinstance(value, EntityStatus):
            raise ValueError('Invalid type for field status, expected EntityStatus')

        self.msg.status.CopyFrom(value)
        return self

    def set_table_group_name(self, value):
        self.msg.table_group_name = value
        return self

    def set_database_name(self, value):
        self.msg.database_name = value
        return self

    def set_catalog_name(self, value):
        self.msg.catalog_name = value
        return self

    def set_table_group_config(self, value):
        if not isinstance(value, TableGroupConfig):
            raise ValueError('Invalid type for field table_group_config, expected TableGroupConfig')

        self.msg.table_group_config.CopyFrom(value)
        return self

    def add_load_strategy(self, value):
        if not isinstance(value, LoadStrategy):
            raise ValueError('Invalid type for field load_strategies, expected LoadStrategy')

        load_strategy = self.self.msg.load_strategies.add()
        load_strategy.CopyFrom(value)
        return self

    def set_load_strategies(self, values):
        for value in values:
            self.add_load_strategy(value)
        return self

    def set_operation_meta(self, value):
        if not isinstance(value, OperationMeta):
            raise ValueError('Invalid type for field operation_meta, expected OperationMeta')

        self.msg.operation_meta.CopyFrom(value)
        return self


class CatalogConfigBuilder(BaseBuilder):
    def __init__(self):
        self.msg = CatalogConfig()

    def set_version(self, value):
        self.msg.version = value
        return self

    def set_status(self, value):
        if not isinstance(value, EntityStatus):
            raise ValueError('Invalid type for field status, expected EntityStatus')

        self.msg.status.CopyFrom(value)
        return self

    def set_workspace_name(self, value):
        self.msg.workspace_name = value
        return self

    def set_data_space_name(self, value):
        self.msg.data_space_name = value
        return self


class CatalogBuilder(BaseBuilder):
    def __init__(self):
        super(CatalogBuilder, self).__init__(Catalog())

    def set_version(self, value):
        self.msg.version = value
        return self

    def set_status(self, value):
        if not isinstance(value, EntityStatus):
            raise ValueError('Invalid type for field status, expected EntityStatus')

        self.msg.status.CopyFrom(value)
        return self

    def set_catalog_name(self, value):
        self.msg.catalog_name = value
        return self

    def set_catalog_config(self, value):
        if not isinstance(value, CatalogConfig):
            raise ValueError('Invalid type for field catalog_config, expected CatalogConfig')

        self.msg.catalog_config.CopyFrom(value)
        return self

    def add_database(self, value):
        if not isinstance(value, Database):
            raise ValueError('Invalid type for field databases, expected Database')

        database = self.msg.databases.add()
        database.CopyFrom(value)
        return self

    def set_databases(self, values):
        for value in values:
            self.add_database(value)
        return self

    def set_operation_meta(self, value):
        if not isinstance(value, OperationMeta):
            raise ValueError('Invalid type for field operation_meta, expected OperationMeta')

        self.msg.operation_meta.CopyFrom(value)
        return self
