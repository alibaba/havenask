load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_pack_attribute_test_helper',
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/common/field_format/pack_attribute/test'
)
cc_fast_test(
    name='indexlib_pack_attribute_unittest',
    srcs=glob(['*unittest.cpp', '*Test.cpp']),
    copts=['-Werror', '-fno-access-control'],
    data=(['//aios/storage/indexlib:testdata'] + glob(['testdata/**'])),
    deps=[
        ':indexlib_pack_attribute_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib:indexlib_testlib',
        '//aios/storage/indexlib/indexlib/common',
        '//aios/storage/indexlib/indexlib/config/test:indexlib_config_test_helper',
        '//aios/storage/indexlib/indexlib/document'
    ]
)
