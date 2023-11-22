cc_library(
    name='options',
    srcs=[
        'build_config.cpp', 'build_config_base.cpp',
        'disable_fields_config.cpp', 'document_deduper_helper.cpp',
        'impl/build_config_impl.cpp', 'impl/index_partition_options_impl.cpp',
        'impl/merge_config_impl.cpp', 'impl/offline_config_impl.cpp',
        'impl/online_config_impl.cpp', 'index_partition_options.cpp',
        'kv_online_config.cpp', 'merge_config.cpp', 'merge_config_base.cpp',
        'merge_strategy_parameter.cpp', 'offline_config.cpp',
        'offline_config_base.cpp', 'online_config.cpp', 'online_config_base.cpp'
    ],
    hdrs=[
        'build_config.h', 'build_config_base.h', 'document_deduper_helper.h',
        'file_system_config.h', 'impl/build_config_impl.h',
        'impl/index_partition_options_impl.h', 'impl/merge_config_impl.h',
        'impl/offline_config_impl.h', 'impl/online_config_impl.h',
        'index_partition_options.h', 'kv_online_config.h', 'merge_config.h',
        'merge_config_base.h', 'merge_io_config.h',
        'merge_strategy_parameter.h', 'offline_config.h',
        'offline_config_base.h', 'online_config.h', 'online_config_base.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/config',
    deps=['//aios/storage/indexlib/legacy/config:schema']
)
cc_library(
    name='config',
    hdrs=['function_executor_resource.h'],
    copts=['-Werror'],
    include_prefix='indexlib/config',
    visibility=['//aios/storage/indexlib/indexlib:indexlibV1'],
    deps=[':options', '//aios/storage/indexlib/legacy/config:schema']
)
