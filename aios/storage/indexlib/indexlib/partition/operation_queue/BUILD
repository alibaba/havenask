package(
    default_visibility=[
        '//aios/storage/indexlib/indexlib/partition:__subpackages__'
    ]
)
cc_library(
    name='operation-queue',
    srcs=[
        'compress_operation_block.cpp', 'compress_operation_writer.cpp',
        'dump_operation_redo_strategy.cpp', 'file_operation_block.cpp',
        'in_mem_segment_operation_iterator.cpp',
        'normal_segment_operation_iterator.cpp', 'operation_block.cpp',
        'operation_dumper.cpp', 'operation_factory.cpp',
        'operation_iterator.cpp', 'operation_replayer.cpp',
        'operation_writer.cpp', 'optimized_reopen_redo_strategy.cpp',
        'recover_rt_operation_redo_strategy.cpp',
        'remove_operation_creator.cpp', 'reopen_operation_redo_strategy.cpp',
        'segment_operation_iterator.cpp', 'sub_doc_operation.cpp',
        'sub_doc_operation_creator.cpp', 'update_field_operation_creator.cpp'
    ],
    hdrs=[
        'compress_operation_block.h', 'compress_operation_writer.h',
        'dump_operation_redo_strategy.h', 'file_operation_block.h',
        'in_mem_segment_operation_iterator.h',
        'normal_segment_operation_iterator.h', 'operation_base.h',
        'operation_block.h', 'operation_creator.h', 'operation_cursor.h',
        'operation_dumper.h', 'operation_factory.h', 'operation_iterator.h',
        'operation_meta.h', 'operation_redo_hint.h',
        'operation_redo_strategy.h', 'operation_replayer.h',
        'operation_writer.h', 'optimized_reopen_redo_strategy.h',
        'recover_rt_operation_redo_strategy.h', 'remove_operation.h',
        'remove_operation_creator.h', 'reopen_operation_redo_strategy.h',
        'segment_operation_iterator.h', 'sub_doc_operation.h',
        'sub_doc_operation_creator.h', 'update_field_operation.h',
        'update_field_operation_creator.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/partition/operation_queue',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/autil:net', '//aios/storage/indexlib/index/primary_key:util',
        '//aios/storage/indexlib/indexlib/common',
        '//aios/storage/indexlib/indexlib/config',
        '//aios/storage/indexlib/indexlib/index:index_external',
        '//aios/storage/indexlib/indexlib/partition:on-disk-partition-data',
        '//aios/storage/indexlib/indexlib/partition:partition_reader',
        '//aios/storage/indexlib/indexlib/partition/modifier'
    ],
    alwayslink=True
)
