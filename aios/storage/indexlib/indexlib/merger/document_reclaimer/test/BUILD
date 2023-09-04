load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_document_reclaimer_test_helper',
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/merger/document_reclaimer/test'
)
cc_fast_test(
    name='indexlib_document_reclaimer_unittest',
    srcs=glob(['*.cpp']),
    copts=['-Werror'],
    data=['//aios/storage/indexlib:testdata'],
    deps=[
        ':indexlib_document_reclaimer_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper'
    ]
)
