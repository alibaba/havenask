load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_config_test_helper',
    srcs=['schema_loader.cpp', 'truncate_config_maker.cpp'],
    hdrs=[
        'attribute_config_unittest.h', 'build_config_unittest.h',
        'customized_index_config_unittest.h', 'date_index_config_unittest.h',
        'field_config_unittest.h', 'field_type_traits_unittest.h',
        'high_freq_vocabulary_creator_unittest.h', 'index_config_unittest.h',
        'index_partition_options_unittest.h',
        'index_partition_schema_unittest.h', 'kkv_index_config_unittest.h',
        'kv_index_config_unittest.h', 'merge_config_unittest.h',
        'merge_io_config_unittest.h', 'online_config_unittest.h',
        'pack_attribute_config_unittest.h', 'package_index_config_unittest.h',
        'primary_key_index_config_unittest.h', 'range_index_config_unittest.h',
        'schema_differ_unittest.h', 'schema_loader.h',
        'section_attribute_config_unittest.h',
        'spatial_index_config_unittest.h', 'summary_schema_unittest.h',
        'truncate_config_maker.h', 'truncate_option_config_unittest.h',
        'value_config_unittest.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/config/test',
    visibility=['//aios/storage/indexlib/indexlib:indexlibV1'],
    deps=['//aios/storage/indexlib/indexlib/config']
)
cc_fast_test(
    name='indexlib_config_unittest',
    srcs=[
        'attribute_config_unittest.cpp', 'build_config_unittest.cpp',
        'check_compability_unittest.cpp', 'check_config_unittest.cpp',
        'customized_index_config_unittest.cpp',
        'date_index_config_unittest.cpp', 'diversity_constrain_unittest.cpp',
        'field_config_unittest.cpp', 'field_type_traits_unittest.cpp',
        'high_freq_vocabulary_creator_unittest.cpp',
        'index_config_unittest.cpp', 'index_partition_options_unittest.cpp',
        'index_partition_schema_unittest.cpp', 'kkv_index_config_unittest.cpp',
        'kv_index_config_unittest.cpp', 'merge_config_unittest.cpp',
        'merge_io_config_unittest.cpp', 'module_info_unittest.cpp',
        'online_config_unittest.cpp', 'pack_attribute_config_unittest.cpp',
        'package_index_config_unittest.cpp',
        'primary_key_index_config_unittest.cpp',
        'range_index_config_unittest.cpp', 'schema_configurator_unittest.cpp',
        'schema_differ_unittest.cpp', 'section_attribute_config_unittest.cpp',
        'single_field_index_config_unittest.cpp', 'source_schema_unittest.cpp',
        'spatial_index_config_unittest.cpp', 'summary_schema_unittest.cpp',
        'temperature_layer_config_unittest.cpp',
        'truncate_index_name_mapper_unittest.cpp',
        'truncate_option_config_unittest.cpp',
        'truncate_term_vocabulary_unittest.cpp', 'value_config_unittest.cpp'
    ],
    copts=['-Werror', '-fno-access-control'],
    data=(['//aios/storage/indexlib:testdata'] + glob(['testdata/**'])),
    shard_count=2,
    deps=[
        ':indexlib_config_test_helper',
        '//aios/storage/indexlib/indexlib/config',
        '//aios/storage/indexlib/legacy/config/test:schema_maker',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
