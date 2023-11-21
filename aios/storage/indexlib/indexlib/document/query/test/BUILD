load('@rules_cc//cc:defs.bzl', 'cc_library')
load('//bazel:defs.bzl', 'cc_fast_test', 'test_cc_plugin_shared_library')
test_cc_plugin_shared_library(
    name='example_function_executor',
    srcs=['example_function_executor.cpp'],
    hdrs=['example_function_executor.h'],
    copy_dests=['testdata/plugins/'],
    include_prefix='indexlib/document/query/test',
    preloaded_deps=['//aios/storage/indexlib/indexlib/document:document_query'],
    static_deps=[],
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__']
)
cc_library(
    name='indexlib_document_query_test_helper',
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/document/query/test'
)
cc_fast_test(
    name='indexlib_document_query_unittest',
    srcs=glob(['*unittest.cpp']),
    copts=['-fno-access-control'],
    data=([':example_function_executor_testdata'] + glob(['testdata/**'])),
    deps=[
        ':indexlib_document_query_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/document:document_query'
    ]
)
