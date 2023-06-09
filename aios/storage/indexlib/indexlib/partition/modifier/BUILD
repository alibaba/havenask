package(
    default_visibility=[
        '//aios/storage/indexlib/indexlib/partition:__subpackages__'
    ]
)
cc_library(
    name='modifier',
    srcs=[
        'in_memory_segment_modifier.cpp', 'inplace_modifier.cpp',
        'main_sub_modifier_util.cpp', 'offline_modifier.cpp',
        'partition_modifier.cpp', 'partition_modifier_creator.cpp',
        'partition_modifier_task_item.cpp', 'patch_modifier.cpp',
        'sub_doc_modifier.cpp'
    ],
    hdrs=[
        'in_memory_segment_modifier.h', 'inplace_modifier.h',
        'main_sub_modifier_util.h', 'offline_modifier.h',
        'partition_modifier.h', 'partition_modifier_creator.h',
        'partition_modifier_task_item.h', 'patch_modifier.h',
        'sub_doc_modifier.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/partition/modifier',
    deps=[
        '//aios/storage/indexlib/indexlib/index',
        '//aios/storage/indexlib/indexlib/index:index_external',
        '//aios/storage/indexlib/indexlib/index/normal/attribute:appender',
        '//aios/storage/indexlib/indexlib/index/normal/attribute:attribute_modifier',
        '//aios/storage/indexlib/indexlib/partition:partition_reader'
    ]
)
