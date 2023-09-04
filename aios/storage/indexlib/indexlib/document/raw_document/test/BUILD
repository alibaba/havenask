load('//bazel:defs.bzl', 'cc_fast_test')
cc_fast_test(
    name='indexlib_raw_document_unittest',
    srcs=glob(['*unittest.cpp']),
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    deps=[
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/document:raw_document'
    ]
)
