load('//bazel:defs.bzl', 'cc_fast_test')
cc_fast_test(
    name='indexlib_kv_parser_unittest',
    srcs=glob(['*unittest.cpp', '*.h']),
    copts=['-Werror', '-fno-access-control'],
    data=glob(['testdata/**']),
    deps=[
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/config/test:indexlib_config_test_helper',
        '//aios/storage/indexlib/indexlib/document:document_parser',
        '//aios/storage/indexlib/indexlib/test:document',
        '//aios/storage/indexlib/indexlib/test:schema_maker'
    ]
)
