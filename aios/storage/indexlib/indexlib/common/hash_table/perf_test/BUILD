load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_hash_table_perf_test_helper',
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/common/hash_table/perf_test',
    deps=['//aios/storage/indexlib/indexlib/common']
)
cc_fast_test(
    name='indexlib_hash_table_perf_unittest',
    srcs=glob([
        'dense_hash_table_multi_thread_unittest.cpp',
        'cuckoo_hash_table_multi_thread_unittest.cpp'
    ]),
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    shard_count=2,
    deps=[
        ':indexlib_hash_table_perf_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/common/hash_table/test:indexlib_hash_table_test_helper'
    ]
)
