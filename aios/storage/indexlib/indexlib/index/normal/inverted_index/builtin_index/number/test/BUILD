load('//bazel:defs.bzl', 'cc_fast_test')
cc_fast_test(
    name='indexlib_number_unittest',
    srcs=glob(['*unittest.cpp']),
    copts=['-Werror'],
    data=['//aios/storage/indexlib:testdata'],
    shard_count=5,
    deps=[
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index/normal/attribute/test:indexlib_attribute_test_helper',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper'
    ]
)
