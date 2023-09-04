load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_range_test_helper',
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/common/field_format/range/test'
)
cc_fast_test(
    name='indexlib_range_unittest',
    srcs=glob(['*unittest.cpp']),
    copts=['-Werror'],
    deps=[
        ':indexlib_range_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/common:field_format'
    ]
)
