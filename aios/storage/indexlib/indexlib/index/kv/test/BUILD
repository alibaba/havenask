load('//bazel:defs.bzl', 'cc_fast_test')
load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_kv_test_helper',
    hdrs=[
        'create_hash_table_unittest.h', 'create_writer_unittest.h',
        'hash_table_fix_segment_reader_unittest.h',
        'hash_table_fix_writer_unittest.h', 'hash_table_var_writer_unittest.h',
        'kv_doc_timestamp_decider_unittest.h', 'kv_factory_unittest.h',
        'kv_index_options_unittest.h', 'kv_merger_unittest.h',
        'kv_doc_reader_unittest.h', 'kv_reader_creator_unittest.h',
        'kv_reader_impl_unittest.h', 'kv_reader_unittest.h',
        'kv_ttl_decider_unittest.h', 'offline_segment_iterator_unittest.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/index/kv/test'
)
cc_fast_test(
    name='indexlib_kv_unittest',
    srcs=[
        'create_hash_table_unittest.cpp', 'create_writer_unittest.cpp',
        'hash_table_fix_segment_reader_unittest.cpp',
        'hash_table_fix_writer_unittest.cpp',
        'hash_table_var_writer_unittest.cpp',
        'kv_doc_timestamp_decider_unittest.cpp', 'kv_factory_unittest.cpp',
        'kv_index_options_unittest.cpp', 'kv_merger_unittest.cpp',
        'kv_doc_reader_unittest.cpp', 'kv_reader_impl_unittest.cpp',
        'kv_reader_unittest.cpp', 'kv_ttl_decider_unittest.cpp',
        'offline_segment_iterator_unittest.cpp'
    ],
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    shard_count=10,
    deps=[
        ':indexlib_kv_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/file_system/test:test-util',
        '//aios/storage/indexlib/indexlib/index:kv',
        '//aios/storage/indexlib/indexlib/partition:indexlib_partition',
        '//aios/storage/indexlib/indexlib/test:partition_state_machine',
        '//aios/storage/indexlib/indexlib/test:schema_maker'
    ]
)
cc_fast_test(
    name='todo',
    srcs=[],
    copts=['-Werror'],
    data=['//aios/storage/indexlib:testdata'],
    shard_count=10,
    deps=[
        ':indexlib_kv_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper'
    ]
)
