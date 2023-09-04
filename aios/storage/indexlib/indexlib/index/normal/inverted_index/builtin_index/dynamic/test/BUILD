load('//bazel:defs.bzl', 'cc_fast_test')
cc_fast_test(
    name='indexlib_dynamic_inverted_index_unittest',
    srcs=glob([
        'dynamic_index_writer_unittest.cpp', 'dynamic_index_unittest.cpp',
        'dynamic_index_reader_unittest.cpp'
    ]),
    copts=['-Werror'],
    data=['//aios/storage/indexlib:testdata'],
    deps=[
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib:indexlib_testbase_helper',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper',
        '//aios/storage/indexlib/indexlib/util/test:indexlib_util_test_helper'
    ]
)
