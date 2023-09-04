load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_kv_document_test_helper',
    testonly=True,
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/document/kv_document/test',
    deps=[
        '//aios/storage/indexlib/indexlib:indexlib_testbase_helper',
        '//aios/storage/indexlib/indexlib/config/test:indexlib_config_test_helper',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper'
    ]
)
cc_fast_test(
    name='indexlib_kv_document_unittest',
    srcs=['kv_document_unittest.cpp'],
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    deps=[
        ':indexlib_kv_document_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/document:document_parser'
    ]
)
