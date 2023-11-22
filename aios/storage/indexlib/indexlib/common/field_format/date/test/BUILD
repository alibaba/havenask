load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_date_test_helper',
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/common/field_format/date/test'
)
cc_fast_test(
    name='indexlib_date_unittest',
    srcs=glob(['*.cpp']),
    copts=['-fno-access-control'],
    deps=[
        ':indexlib_date_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib:indexlib_testlib',
        '//aios/storage/indexlib/indexlib/common'
    ]
)
