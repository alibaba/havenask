load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_deletionmap_test_helper',
    hdrs=glob(['*h']),
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/deletionmap/test'
)
cc_fast_test(
    name='indexlib_deletionmap_unittest',
    srcs=glob(['*unittest.cpp']),
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    deps=[
        ':indexlib_deletionmap_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_merger',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index/normal/deletionmap',
        '//aios/storage/indexlib/indexlib/test:index_meta_maker',
        '//aios/storage/indexlib/indexlib/test:partition_state_machine',
        '//aios/storage/indexlib/indexlib/util/test:indexlib_util_test_helper'
    ]
)
