cc_library(
    name='partition_reader',
    srcs=['index_partition_reader.cpp', 'partition_version.cpp'],
    hdrs=['index_partition_reader.h', 'partition_version.h'],
    copts=['-Werror'],
    include_prefix='indexlib/partition',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=['//aios/storage/indexlib/indexlib/index_base']
)
cc_library(
    name='table_reader_container',
    srcs=['table_reader_container.cpp', 'table_reader_container_updater.cpp'],
    hdrs=['table_reader_container.h', 'table_reader_container_updater.h'],
    copts=['-Werror'],
    include_prefix='indexlib/partition',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':partition_reader',
        '//aios/storage/indexlib/indexlib/partition/join_cache:base',
        '//aios/storage/indexlib/indexlib/partition/join_cache:join_cache_initializer'
    ]
)
cc_library(
    name='index_roll_back_util',
    srcs=['index_roll_back_util.cpp'],
    hdrs=['index_roll_back_util.h'],
    copts=['-Werror'],
    include_prefix='indexlib/partition',
    visibility=['//visibility:public'],
    deps=['//aios/storage/indexlib/indexlib/index_base'],
    alwayslink=True
)
cc_library(
    name='partition-info-holder',
    srcs=['partition_info_holder.cpp'],
    hdrs=['partition_info_holder.h'],
    copts=['-Werror'],
    include_prefix='indexlib/partition',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/indexlib/index:index_external',
        '//aios/storage/indexlib/indexlib/index/normal/deletionmap',
        '//aios/storage/indexlib/indexlib/index_base'
    ],
    alwayslink=True
)
cc_library(
    name='on-disk-partition-data',
    srcs=['on_disk_partition_data.cpp'],
    hdrs=['on_disk_partition_data.h'],
    copts=['-Werror'],
    include_prefix='indexlib/partition',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':partition-info-holder',
        '//aios/storage/indexlib/indexlib/index:index_external',
        '//aios/storage/indexlib/indexlib/index/normal/deletionmap',
        '//aios/storage/indexlib/indexlib/index_base'
    ],
    alwayslink=True
)
cc_library(
    name='partition_interface',
    srcs=[
        'builder_branch_hinter.cpp', 'flushed_locator_container.cpp',
        'group_memory_reporter.cpp', 'index_partition.cpp',
        'index_partition_resource.cpp', 'local_index_cleaner.cpp',
        'memory_stat_collector.cpp', 'memory_stat_reporter.cpp',
        'online_partition_metrics.cpp', 'partition_group_resource.cpp',
        'reader_container.cpp'
    ],
    hdrs=[
        'builder_branch_hinter.h', 'flushed_locator_container.h',
        'group_memory_reporter.h', 'index_partition.h',
        'index_partition_resource.h', 'local_index_cleaner.h',
        'memory_stat_collector.h', 'memory_stat_reporter.h',
        'online_partition_metrics.h', 'partition_group_resource.h',
        'reader_container.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/partition',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':partition_reader',
        '//aios/storage/indexlib/indexlib/index:index_external',
        '//aios/storage/indexlib/indexlib/index:index_metrics',
        '//aios/storage/indexlib/indexlib/index/normal/attribute:metrics',
        '//aios/storage/indexlib/indexlib/index_base'
    ]
)
cc_library(
    name='reader_sdk',
    srcs=['index_application.cpp', 'partition_reader_snapshot.cpp'],
    hdrs=[
        'aux_table_reader_creator.h', 'aux_table_reader_typed.h',
        'index_application.h', 'partition_define.h',
        'partition_reader_snapshot.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/partition',
    visibility=['//visibility:public'],
    deps=[
        ':partition_interface', ':table_reader_container',
        '//aios/storage/indexlib/framework:ITablet',
        '//aios/storage/indexlib/framework:ITabletReader',
        '//aios/storage/indexlib/framework:TabletInfos',
        '//aios/storage/indexlib/index/primary_key:reader',
        '//aios/storage/indexlib/index/primary_key:util',
        '//aios/storage/indexlib/indexlib/index:kkv',
        '//aios/storage/indexlib/indexlib/index:kv_reader_interface',
        '//aios/storage/indexlib/indexlib/partition/join_cache:join_cache_sdk',
        '//aios/storage/indexlib/table/kv_table:reader',
        '//aios/storage/indexlib/table/normal_table:NormalTabletSessionReader'
    ]
)
cc_library(
    name='main-sub-util',
    srcs=['main_sub_util.cpp'],
    hdrs=['main_sub_util.h'],
    copts=['-Werror'],
    include_prefix='indexlib/partition',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/indexlib/index',
        '//aios/storage/indexlib/indexlib/partition/modifier',
        '//aios/storage/indexlib/indexlib/partition/segment:segment-writer'
    ]
)
cc_library(
    name='indexlib_partition',
    srcs=(
        glob(['*.cpp', 'open_executor/*.cpp', 'remote_access/*.cpp'],
             exclude=[
                 'build_document_metrics.cpp', 'builder_branch_hinter.cpp',
                 'index_partition_reader.cpp', 'partition_version.cpp',
                 'index_partition.cpp', 'flushed_locator_container.cpp',
                 'index_partition_resource.cpp', 'partition_group_resource.cpp',
                 'local_index_cleaner.cpp', 'memory_stat_reporter.cpp',
                 'memory_stat_collector.cpp', 'main_sub_util.cpp',
                 'document_collector.cpp', 'on_disk_partition_data.cpp',
                 'partition_info_holder.cpp', 'group_memory_reporter.cpp',
                 'online_partition_metrics.cpp', 'reader_container.cpp',
                 'table_reader_container.cpp',
                 'table_reader_container_updater.cpp',
                 'index_roll_back_util.cpp', 'index_application.cpp',
                 'partition_reader_snapshot.cpp'
             ]) +
        ['//aios/storage/indexlib/indexlib/partition/segment:dump-srcs']
    ),
    hdrs=(
        glob(['*.h', 'open_executor/*.h', 'remote_access/*.h'],
             exclude=[
                 'builder_branch_hinter.h', 'index_partition_reader.h',
                 'partition_version.h', 'index_partition.h',
                 'flushed_locator_container.h', 'index_partition_resource.h',
                 'partition_group_resource.h', 'local_index_cleaner.h',
                 'memory_stat_reporter.h', 'memory_stat_collector.h',
                 'group_memory_reporter.h', 'main_sub_util.h',
                 'document_collector.h', 'on_disk_partition_data.h',
                 'partition_info_holder.h', 'online_partition_metrics.h',
                 'reader_container.h', 'table_reader_container.h',
                 'table_reader_container_updater.h', 'index_roll_back_util.h',
                 'index_application.h', 'partition_define.h',
                 'partition_reader_snapshot.h', 'aux_table_reader_creator.h',
                 'aux_table_reader_typed.h'
             ]) +
        ['//aios/storage/indexlib/indexlib/partition/segment:dump-hdrs']
    ),
    copts=['-Werror'],
    include_prefix='indexlib/partition',
    visibility=['//visibility:public'],
    deps=[
        ':main-sub-util', ':metrics', ':on-disk-partition-data',
        ':partition-info-holder', ':partition_interface', ':partition_reader',
        ':reader_sdk', ':table_reader_container', '//aios/autil:net',
        '//aios/storage/indexlib/framework:ITablet',
        '//aios/storage/indexlib/framework:ITabletReader',
        '//aios/storage/indexlib/indexlib:indexlib_merger',
        '//aios/storage/indexlib/indexlib:indexlib_table',
        '//aios/storage/indexlib/indexlib/common',
        '//aios/storage/indexlib/indexlib/config',
        '//aios/storage/indexlib/indexlib/document',
        '//aios/storage/indexlib/indexlib/index',
        '//aios/storage/indexlib/indexlib/index:calculator',
        '//aios/storage/indexlib/indexlib/partition/document_deduper:document-deduper',
        '//aios/storage/indexlib/indexlib/partition/join_cache:join_cache_initializer',
        '//aios/storage/indexlib/indexlib/partition/modifier',
        '//aios/storage/indexlib/indexlib/partition/operation_queue:operation-queue',
        '//aios/storage/indexlib/indexlib/partition/segment',
        '//aios/storage/indexlib/table/normal_table:NormalTabletSessionReader'
    ],
    alwayslink=True
)
cc_library(
    name='metrics',
    srcs=['build_document_metrics.cpp'],
    hdrs=['build_document_metrics.h'],
    copts=['-Werror'],
    include_prefix='indexlib/partition',
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__'],
    deps=['//aios/storage/indexlib/indexlib/document']
)
