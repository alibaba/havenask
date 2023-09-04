load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_normal_parser_test_helper',
    testonly=True,
    srcs=glob(['*.cpp'], exclude=['*unittest.cpp']),
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/document/document_parser/normal_parser/test',
    deps=[
        '//aios/storage/indexlib/indexlib:indexlib_testbase_helper',
        '//aios/storage/indexlib/indexlib/config/test:indexlib_config_test_helper',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper'
    ]
)
cc_fast_test(
    name='indexlib_normal_parser_unittest',
    srcs=glob(['*unittest.cpp']),
    copts=['-Werror', '-fno-access-control'],
    data=glob(['testdata/**']),
    deps=[
        ':indexlib_normal_parser_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/document:document_parser'
    ]
)
