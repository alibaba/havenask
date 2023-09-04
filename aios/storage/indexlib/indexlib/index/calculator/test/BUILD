load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_calculator_test_helper',
    hdrs=glob(['*h']),
    copts=['-Werror'],
    include_prefix='indexlib/index/calculator/test'
)
cc_fast_test(
    name='indexlib_calculator_unittest',
    srcs=glob(['*.cpp']),
    copts=['-Werror', '-fno-access-control'],
    data=(['//aios/storage/indexlib:testdata'] + glob(['testdata/**'])),
    shard_count=2,
    deps=[
        ':indexlib_calculator_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index/normal/attribute/test:indexlib_attribute_test_helper'
    ]
)
