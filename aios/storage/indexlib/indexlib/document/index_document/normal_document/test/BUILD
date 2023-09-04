load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_normal_document_test_helper',
    testonly=True,
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/document/index_document/normal_document/test',
    deps=[
        '//aios/storage/indexlib/indexlib:indexlib_testbase_helper',
        '//aios/storage/indexlib/indexlib/config/test:indexlib_config_test_helper',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper'
    ]
)
cc_fast_test(
    name='indexlib_normal_document_unittest',
    srcs=[
        'attribute_document_field_extractor_unittest.cpp',
        'attribute_document_unittest.cpp', 'field_builder_unittest.cpp',
        'field_unittest.cpp', 'index_document_unittest.cpp',
        'modified_tokens_unittest.cpp', 'normal_document_unittest.cpp',
        'search_summary_document_unittest.cpp', 'section_unittest.cpp',
        'serialized_summary_document_unittest.cpp',
        'source_document_formatter_unittest.cpp',
        'source_document_unittest.cpp', 'summary_document_unittest.cpp',
        'token_unittest.cpp'
    ],
    copts=['-Werror', '-fno-access-control'],
    data=(['//aios/storage/indexlib:testdata'] + glob(['testdata/**'])),
    deps=[
        ':indexlib_normal_document_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/document:document_parser'
    ]
)
