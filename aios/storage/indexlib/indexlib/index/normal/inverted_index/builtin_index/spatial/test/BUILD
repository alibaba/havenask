load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_spatial_test_helper',
    hdrs=glob(['*h']),
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/inverted_index/builtin_index/spatial/test'
)
cc_fast_test(
    name='indexlib_spatial_unittest',
    srcs=glob(['*.cpp']),
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    flaky=True,
    deps=[
        ':indexlib_spatial_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper',
        '//aios/storage/indexlib/indexlib/util/test:indexlib_util_test_helper'
    ]
)
