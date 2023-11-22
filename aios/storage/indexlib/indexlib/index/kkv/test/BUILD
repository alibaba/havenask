load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_kkv_test_helper',
    hdrs=glob(['*h']),
    copts=['-Werror'],
    include_prefix='indexlib/index/kkv/test'
)
cc_fast_test(
    name='indexlib_kkv_unittest',
    srcs=glob(['*.cpp']),
    copts=['-Werror', '-fno-access-control'],
    data=(['//aios/storage/indexlib:testdata'] + glob(['testdata/**'])),
    shard_count=10,
    deps=[
        ':indexlib_kkv_test_helper',
        '//aios/future_lite/future_lite/executors:simple_executor',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper'
    ]
)
