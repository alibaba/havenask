load('//bazel:defs.bzl', 'cc_fast_test', 'test_cc_plugin_shared_library')
test_cc_plugin_shared_library(
    name='example_src_parser',
    srcs=['example_src_schema_parser.cpp'],
    hdrs=['example_src_schema_parser.h'],
    copy_dests=['testdata/plugins/'],
    include_prefix='indexlib/document/document_parser/source_schema_parser/test',
    preloaded_deps=[
        '//aios/storage/indexlib/indexlib/document:document_parser'
    ],
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__']
)
cc_fast_test(
    name='indexlib_source_schema_parser_unittest',
    srcs=glob(['*unittest.cpp']),
    copts=['-Werror', '-fno-access-control'],
    data=[
        'testdata/schema_with_source.json',
        'testdata/schema_with_source_exception.json',
        ':example_src_parser_testdata', '//aios/storage/indexlib:testdata'
    ],
    deps=[
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/document:document_parser'
    ]
)
