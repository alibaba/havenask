load('//bazel:defs.bzl', 'cc_fast_test')
cc_fast_test(
    name='indexlib_text_unittest',
    srcs=glob(['*.cpp']),
    copts=['-Werror'],
    data=['//aios/storage/indexlib:testdata'],
    shard_count=2,
    deps=[
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper'
    ]
)
