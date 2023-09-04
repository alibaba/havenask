load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_customized_index_field_encoder_test_helper',
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/common/field_format/customized_index/test'
)
cc_fast_test(
    name='indexlib_customized_index_field_encoder_unittest',
    srcs=glob(['*unittest.cpp']),
    data=['//aios/storage/indexlib:testdata'],
    deps=[
        ':indexlib_customized_index_field_encoder_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib:indexlib_testlib',
        '//aios/storage/indexlib/indexlib/common:field_format'
    ]
)
