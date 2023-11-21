load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_section_attribute_test_helper',
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/common/field_format/section_attribute/test'
)
cc_fast_test(
    name='indexlib_section_attribute_unittest',
    srcs=glob(['*unittest.cpp']),
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    flaky=True,
    deps=[
        ':indexlib_section_attribute_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/common',
        '//aios/storage/indexlib/indexlib/index',
        '//aios/storage/indexlib/indexlib/index:kkv',
        '//aios/storage/indexlib/indexlib/index:kv'
    ]
)
