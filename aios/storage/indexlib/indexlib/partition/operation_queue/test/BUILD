load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_operation_queue_test_helper',
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/partition/operation_queue/test',
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__']
)
cc_fast_test(
    name='indexlib_operation_queue_unittest',
    srcs=[
        'compress_operation_writer_unittest.cpp',
        'operation_block_unittest.cpp', 'operation_factory_unittest.cpp',
        'operation_iterator_unittest.cpp',
        'operation_redo_strategy_unittest.cpp', 'operation_writer_unittest.cpp',
        'optimized_reopen_redo_strategy_unittest.cpp',
        'remove_operation_creator_unittest.cpp',
        'remove_operation_unittest.cpp',
        'sub_doc_operation_creator_unittest.cpp',
        'sub_doc_operation_unittest.cpp',
        'update_field_operation_creator_unittest.cpp',
        'update_field_operation_unittest.cpp'
    ],
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    deps=[
        ':indexlib_operation_queue_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper',
        '//aios/storage/indexlib/indexlib/partition/test:indexlib_partition_test_helper'
    ]
)
cc_fast_test(
    name='todo',
    srcs=[],
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    deps=[
        ':indexlib_operation_queue_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper',
        '//aios/storage/indexlib/indexlib/partition/test:indexlib_partition_test_helper'
    ]
)
