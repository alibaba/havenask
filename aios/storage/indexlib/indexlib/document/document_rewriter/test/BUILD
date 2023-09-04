load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_document_rewriter_test_helper',
    testonly=True,
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/document/document_rewriter/test',
    deps=[
        '//aios/storage/indexlib/indexlib:indexlib_testbase_helper',
        '//aios/storage/indexlib/indexlib/config/test:indexlib_config_test_helper',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper'
    ]
)
cc_fast_test(
    name='indexlib_document_rewriter_unittest',
    srcs=glob(['*unittest.cpp']),
    copts=['-Werror'],
    data=(['//aios/storage/indexlib:testdata'] + glob(['testdata/**'])),
    deps=[
        ':indexlib_document_rewriter_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/document:document_rewriter'
    ]
)
