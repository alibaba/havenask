load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_adaptive_bitmap_test_helper',
    testonly=True,
    srcs=['posting_iterator_mocker.cpp'],
    hdrs=glob(['*h']),
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/adaptive_bitmap/test',
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/indexlib',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper'
    ]
)
cc_fast_test(
    name='indexlib_adaptive_bitmap_unittest',
    srcs=glob(['*unittest.cpp', '*intetest.cpp']),
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    deps=[
        ':indexlib_adaptive_bitmap_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/config/test:indexlib_config_test_helper',
        '//aios/storage/indexlib/indexlib/util/test:indexlib_util_test_helper'
    ]
)
