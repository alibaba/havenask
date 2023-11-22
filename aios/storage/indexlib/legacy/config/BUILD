load('//aios/storage:defs.bzl', 'strict_cc_library')
strict_cc_library(
    name='OfflineMergeConfig',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=['//aios/autil:json', '//aios/storage/indexlib/config:MergeConfig']
)
strict_cc_library(
    name='module_info',
    srcs=['module_info.cpp'],
    hdrs=['module_info.h'],
    include_prefix='indexlib/config',
    visibility=[
        '//aios/apps/facility/build_service:__subpackages__',
        '//aios/storage/indexlib:__subpackages__'
    ],
    deps=['//aios/storage/indexlib/util:key_value_map']
)
strict_cc_library(
    name='temperature',
    srcs=[
        'condition_filter.cpp', 'condition_filter_function_checker.cpp',
        'impl/condition_filter_impl.cpp', 'impl/temperature_condition_impl.cpp',
        'impl/temperature_layer_config_impl.cpp', 'temperature_condition.cpp',
        'temperature_layer_config.cpp'
    ],
    hdrs=[
        'condition_filter.h', 'condition_filter_function_checker.h',
        'impl/condition_filter_impl.h', 'impl/temperature_condition_impl.h',
        'impl/temperature_layer_config_impl.h', 'temperature_condition.h',
        'temperature_layer_config.h'
    ],
    include_prefix='indexlib/config',
    deps=[':attribute_schema', '//aios/autil:log']
)
strict_cc_library(
    name='attribute_schema',
    srcs=[
        'attribute_config.cpp', 'attribute_config_creator.cpp',
        'attribute_schema.cpp', 'configurator_define.cpp',
        'customized_config.cpp', 'enum_field_config.cpp', 'field_schema.cpp',
        'field_type_traits.cpp', 'impl/attribute_schema_impl.cpp',
        'impl/customized_config_impl.cpp', 'impl/field_schema_impl.cpp',
        'modify_item.cpp', 'pack_attribute_config.cpp'
    ],
    hdrs=[
        'attribute_config.h', 'attribute_config_creator.h',
        'attribute_schema.h', 'config_define.h', 'configurator_define.h',
        'customized_config.h', 'enum_field_config.h', 'field_config.h',
        'field_schema.h', 'field_type_traits.h', 'impl/attribute_schema_impl.h',
        'impl/customized_config_impl.h', 'impl/field_schema_impl.h',
        'modify_item.h', 'pack_attribute_config.h'
    ],
    include_prefix='indexlib/config',
    deps=[
        '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/config:FileCompressConfig',
        '//aios/storage/indexlib/config:GroupDataParameter',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/index/attribute:constants',
        '//aios/storage/indexlib/index/attribute/config',
        '//aios/storage/indexlib/index/inverted_index:constants',
        '//aios/storage/indexlib/index/pack_attribute:config',
        '//aios/storage/indexlib/legacy:indexlib',
        '//aios/storage/indexlib/util:status_to_exception'
    ]
)
strict_cc_library(
    name='schema',
    srcs=[
        'adaptive_dictionary_schema.cpp', 'analyzer_config.cpp',
        'customized_index_config.cpp', 'customized_table_config.cpp',
        'date_index_config.cpp', 'dictionary_schema.cpp',
        'diversity_constrain.cpp', 'field_config.cpp',
        'field_config_loader.cpp', 'impl/adaptive_dictionary_schema_impl.cpp',
        'impl/analyzer_config_impl.cpp',
        'impl/customized_table_config_impl.cpp',
        'impl/dictionary_schema_impl.cpp',
        'impl/index_partition_schema_impl.cpp', 'impl/index_schema_impl.cpp',
        'impl/region_schema_impl.cpp', 'impl/schema_modify_operation_impl.cpp',
        'impl/source_schema_impl.cpp', 'impl/summary_group_config_impl.cpp',
        'impl/summary_schema_impl.cpp', 'impl/truncate_index_config_impl.cpp',
        'impl/truncate_profile_schema_impl.cpp', 'impl/value_config_impl.cpp',
        'index_config.cpp', 'index_config_creator.cpp',
        'index_partition_schema.cpp', 'index_partition_schema_maker.cpp',
        'index_schema.cpp', 'kkv_index_config.cpp', 'kv_index_config.cpp',
        'legacy_schema_adapter.cpp', 'meta_re_truncate_config.cpp',
        'module_class_config.cpp', 'number_index_type_transformor.cpp',
        'package_index_config.cpp', 'primary_key_index_config.cpp',
        'range_index_config.cpp', 'region_schema.cpp',
        'schema_configurator.cpp', 'schema_differ.cpp',
        'schema_modify_operation.cpp', 'section_attribute_config.cpp',
        'single_field_index_config.cpp', 'source_schema.cpp',
        'spatial_index_config.cpp', 'summary_group_config.cpp',
        'summary_schema.cpp', 'table_identifier.cpp',
        'truncate_index_config.cpp', 'truncate_index_name_mapper.cpp',
        'truncate_index_property.cpp', 'truncate_option_config.cpp',
        'truncate_profile.cpp', 'truncate_profile_config.cpp',
        'truncate_profile_schema.cpp', 'truncate_strategy.cpp',
        'truncate_term_vocabulary.cpp', 'updateable_schema_standards.cpp',
        'value_config.cpp', 'virtual_attribute_config_creator.cpp'
    ],
    hdrs=[
        'adaptive_dictionary_schema.h', 'analyzer_config.h',
        'customized_index_config.h', 'customized_table_config.h',
        'date_index_config.h', 'dictionary_schema.h', 'disable_fields_config.h',
        'diversity_constrain.h', 'field_config_loader.h',
        'impl/adaptive_dictionary_schema_impl.h', 'impl/analyzer_config_impl.h',
        'impl/customized_table_config_impl.h', 'impl/dictionary_schema_impl.h',
        'impl/index_partition_schema_impl.h', 'impl/index_schema_impl.h',
        'impl/region_schema_impl.h', 'impl/schema_modify_operation_impl.h',
        'impl/source_schema_impl.h', 'impl/summary_group_config_impl.h',
        'impl/summary_schema_impl.h', 'impl/table_identifier_impl.h',
        'impl/truncate_index_config_impl.h',
        'impl/truncate_profile_schema_impl.h', 'impl/value_config_impl.h',
        'index_config.h', 'index_config_creator.h',
        'index_dictionary_bloom_filter_param.h', 'index_partition_schema.h',
        'index_partition_schema_maker.h', 'index_schema.h',
        'kkv_index_config.h', 'kv_index_config.h', 'legacy_schema_adapter.h',
        'load_config_list.h', 'meta_re_truncate_config.h',
        'module_class_config.h', 'number_index_type_transformor.h',
        'package_index_config.h', 'primary_key_index_config.h',
        'primary_key_load_strategy_param.h', 'range_index_config.h',
        'region_schema.h', 'schema_configurator.h', 'schema_differ.h',
        'schema_modify_operation.h', 'section_attribute_config.h',
        'single_field_index_config.h', 'source_group_config.h',
        'source_schema.h', 'spatial_index_config.h', 'summary_group_config.h',
        'summary_schema.h', 'table_identifier.h', 'truncate_index_config.h',
        'truncate_index_name_mapper.h', 'truncate_index_property.h',
        'truncate_option_config.h', 'truncate_profile.h',
        'truncate_profile_config.h', 'truncate_profile_schema.h',
        'truncate_strategy.h', 'truncate_term_vocabulary.h',
        'updateable_schema_standards.h', 'value_config.h',
        'virtual_attribute_config_creator.h'
    ],
    include_prefix='indexlib/config',
    visibility=[
        '//aios/storage/indexlib:__subpackages__',
        '//aios/storage/khronos:__subpackages__'
    ],
    deps=[
        ':attribute_schema', ':module_info', ':temperature',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/config:FieldConfig',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/config:SortDescription',
        '//aios/storage/indexlib/config:SortParam',
        '//aios/storage/indexlib/config:interface',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework:LevelInfo',
        '//aios/storage/indexlib/index/ann:config',
        '//aios/storage/indexlib/index/common:constants',
        '//aios/storage/indexlib/index/deletionmap:constants',
        '//aios/storage/indexlib/index/inverted_index/config:DateIndexConfig',
        '//aios/storage/indexlib/index/inverted_index/config:DateLevelFormat',
        '//aios/storage/indexlib/index/inverted_index/config:InvertedIndexConfig',
        '//aios/storage/indexlib/index/inverted_index/config:PackageIndexConfig',
        '//aios/storage/indexlib/index/inverted_index/config:PayloadConfig',
        '//aios/storage/indexlib/index/inverted_index/config:RangeIndexConfig',
        '//aios/storage/indexlib/index/inverted_index/config:SectionAttributeConfig',
        '//aios/storage/indexlib/index/inverted_index/config:SingleFieldIndexConfig',
        '//aios/storage/indexlib/index/inverted_index/config:SpatialIndexConfig',
        '//aios/storage/indexlib/index/kkv/config',
        '//aios/storage/indexlib/index/kkv/config:KKVIndexFieldInfo',
        '//aios/storage/indexlib/index/kkv/config:KKVIndexPreference',
        '//aios/storage/indexlib/index/kv:constants',
        '//aios/storage/indexlib/index/kv/config',
        '//aios/storage/indexlib/index/primary_key:constants',
        '//aios/storage/indexlib/index/primary_key/config:PrimaryKeyIndexConfig',
        '//aios/storage/indexlib/index/source/config:SourceGroupConfig',
        '//aios/storage/indexlib/index/summary:constants',
        '//aios/storage/indexlib/index/summary/config:SummaryConfig',
        '//aios/storage/indexlib/index/summary/config:SummaryGroupConfig',
        '//aios/storage/indexlib/index/summary/config:SummaryIndexConfig',
        '//aios/storage/indexlib/legacy:indexlib',
        '//aios/storage/indexlib/table:builtin_define',
        '//aios/storage/indexlib/table/normal_table:Common'
    ]
)
