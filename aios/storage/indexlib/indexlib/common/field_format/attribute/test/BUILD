load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_attribute_test_helper',
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/common/field_format/attribute/test'
)
cc_fast_test(
    name='indexlib_attribute_unittest',
    srcs=glob(['*unittest.cpp']),
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    shard_count=5,
    deps=[
        ':indexlib_attribute_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/common:field_format'
    ]
)
