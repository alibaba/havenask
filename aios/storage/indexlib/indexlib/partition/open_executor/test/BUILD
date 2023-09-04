load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_open_executor_helper',
    hdrs=glob(['*h']),
    copts=['-Werror'],
    include_prefix='indexlib/partition/open_executor/test'
)
cc_fast_test(
    name='indexlib_open_executor_unittest',
    srcs=glob(['*.cpp']),
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    shard_count=5,
    deps=[
        ':indexlib_open_executor_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/partition:indexlib_partition',
        '//aios/storage/indexlib/indexlib/partition/test:indexlib_partition_test_helper',
        '//aios/storage/indexlib/indexlib/test:partition_state_machine',
        '//aios/storage/indexlib/indexlib/test:schema_maker',
        '//aios/storage/indexlib/indexlib/util/test:indexlib_util_test_helper'
    ]
)
