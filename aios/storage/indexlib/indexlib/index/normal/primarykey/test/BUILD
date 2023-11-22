package(default_visibility=['//aios/storage/indexlib/indexlib:__subpackages__'])
load('//bazel:defs.bzl', 'cc_slow_test')
cc_library(
    name='primarykey_test_helper',
    testonly=True,
    srcs=[
        'primary_key_formatter_helper.cpp', 'primary_key_test_case_helper.cpp'
    ],
    hdrs=glob(['*h']),
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/primarykey/test',
    deps=[
        '//aios/future_lite/future_lite/executors:simple_executor',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper',
        '//aios/storage/indexlib/indexlib/util/test:indexlib_util_test_helper'
    ]
)
cc_slow_test(
    name='primarykey_slow_unittest',
    srcs=['hash_primary_key_perf_unittest.cpp'],
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    deps=[':primarykey_test_helper']
)
