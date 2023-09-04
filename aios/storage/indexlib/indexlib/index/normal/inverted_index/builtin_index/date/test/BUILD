load('@rules_cc//cc:defs.bzl', 'cc_library')
load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_date_test_helper',
    hdrs=glob(['*h']),
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/inverted_index/builtin_index/date/test'
)
cc_fast_test(
    name='indexlib_date_unittest',
    srcs=glob(['*unittest.cpp']),
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    deps=[
        ':indexlib_date_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper'
    ]
)
