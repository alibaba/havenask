load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_document_deduper_test_helper',
    hdrs=glob(['*h']),
    copts=['-Werror'],
    include_prefix='indexlib/partition/document_deduper/test'
)
cc_fast_test(
    name='indexlib_document_deduper_unittest',
    srcs=glob(['*.cpp']),
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    deps=[
        ':indexlib_document_deduper_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper',
        '//aios/storage/indexlib/indexlib/partition/test:indexlib_partition_test_helper',
        '//aios/storage/indexlib/indexlib/util/test:indexlib_util_test_helper'
    ]
)
