load('//bazel:defs.bzl', 'cc_fast_test')
cc_fast_test(
    name='indexlib_index_datastructure_unittest',
    srcs=glob(['*.cpp']),
    copts=['-Werror'],
    data=['//aios/storage/indexlib:testdata'],
    deps=[
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper'
    ]
)
