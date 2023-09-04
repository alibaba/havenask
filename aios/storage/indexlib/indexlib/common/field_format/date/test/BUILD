load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_date_test_helper',
    hdrs=glob(['*.h']),
    include_prefix='indexlib/common/field_format/date/test',
    copts=['-Werror']
)
cc_fast_test(
    name='indexlib_date_unittest',
    srcs=glob(['*.cpp']),
    deps=[
        ':indexlib_date_test_helper',
        '//aios/storage/indexlib/indexlib/common:common',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib:indexlib_testlib'
    ]
)
