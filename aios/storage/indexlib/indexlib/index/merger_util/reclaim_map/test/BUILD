load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_reclaim_map_test_helper',
    hdrs=glob(['*h']),
    copts=['-Werror'],
    include_prefix='indexlib/index/merger_util/reclaim_map/test'
)
cc_fast_test(
    name='indexlib_reclaim_map_unittest',
    srcs=glob(['*.cpp']),
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    shard_count=5,
    deps=[
        ':indexlib_reclaim_map_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper'
    ]
)
