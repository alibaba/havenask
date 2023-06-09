package(
    default_visibility=[
        '//aios/storage/indexlib/indexlib/partition:__subpackages__'
    ]
)
cc_library(
    name='segment',
    copts=['-Werror'],
    linkstatic=True,
    deps=[
        ':compress-ratio-calculator', ':in-memory-segment',
        ':segment-data-creator', ':segment-sync-item', ':segment-writer'
    ],
    alwayslink=True
)
cc_library(
    name='segment-data-creator',
    srcs=['segment_data_creator.cpp'],
    hdrs=['segment_data_creator.h'],
    copts=['-Werror'],
    include_prefix='indexlib/partition/segment',
    deps=['//aios/autil:net', '//aios/storage/indexlib/indexlib/index_base'],
    alwayslink=True
)
cc_library(
    name='compress-ratio-calculator',
    srcs=['compress_ratio_calculator.cpp'],
    hdrs=['compress_ratio_calculator.h'],
    copts=['-Werror'],
    include_prefix='indexlib/partition/segment',
    deps=[
        '//aios/autil:net', '//aios/storage/indexlib/indexlib/index:index-all',
        '//aios/storage/indexlib/indexlib/index_base'
    ],
    alwayslink=True
)
cc_library(
    name='segment-sync-item',
    srcs=['segment_sync_item.cpp'],
    hdrs=['segment_sync_item.h'],
    copts=['-Werror'],
    include_prefix='indexlib/partition/segment',
    deps=['//aios/autil:net', '//aios/storage/indexlib/indexlib/index'],
    alwayslink=True
)
cc_library(
    name='in-memory-segment',
    srcs=[
        'in_memory_segment_cleaner.cpp', 'in_memory_segment_container.cpp',
        'in_memory_segment_releaser.cpp'
    ],
    hdrs=[
        'in_memory_segment_cleaner.h', 'in_memory_segment_container.h',
        'in_memory_segment_releaser.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/partition/segment',
    deps=[
        ':segment-writer', '//aios/autil:net',
        '//aios/storage/indexlib/indexlib:indexlib_merger',
        '//aios/storage/indexlib/indexlib:indexlib_table',
        '//aios/storage/indexlib/indexlib/common',
        '//aios/storage/indexlib/indexlib/config',
        '//aios/storage/indexlib/indexlib/document',
        '//aios/storage/indexlib/indexlib/index'
    ],
    alwayslink=True
)
cc_library(
    name='segment-writer',
    srcs=[
        'kv_segment_writer.cpp', 'multi_region_kkv_segment_writer.cpp',
        'multi_region_kv_segment_writer.cpp',
        'multi_sharding_segment_writer.cpp', 'segment_writer_util.cpp',
        'single_segment_writer.cpp', 'sub_doc_segment_writer.cpp'
    ],
    hdrs=[
        'kkv_segment_writer.h', 'kv_segment_writer.h',
        'multi_region_kkv_segment_writer.h', 'multi_region_kv_segment_writer.h',
        'multi_sharding_segment_writer.h', 'segment_writer_util.h',
        'single_segment_writer.h', 'sub_doc_segment_writer.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/partition/segment',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/autil:net', '//aios/storage/indexlib/framework:SegmentMeta',
        '//aios/storage/indexlib/indexlib/common',
        '//aios/storage/indexlib/indexlib/config',
        '//aios/storage/indexlib/indexlib/document',
        '//aios/storage/indexlib/indexlib/index:index-all',
        '//aios/storage/indexlib/indexlib/index:segment_metrics_updater',
        '//aios/storage/indexlib/indexlib/partition/modifier'
    ],
    alwayslink=True
)
filegroup(
    name='dump-srcs',
    srcs=[
        'async_segment_dumper.cpp', 'custom_segment_dump_item.cpp',
        'dump_segment_container.cpp', 'dump_segment_queue.cpp',
        'in_memory_segment_creator.cpp', 'normal_segment_dump_item.cpp',
        'segment_dump_item.cpp'
    ]
)
filegroup(
    name='dump-hdrs',
    srcs=[
        'async_segment_dumper.h', 'custom_segment_dump_item.h',
        'dump_segment_container.h', 'dump_segment_queue.h',
        'in_memory_segment_creator.h', 'normal_segment_dump_item.h',
        'segment_dump_item.h'
    ]
)
