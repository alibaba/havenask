load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='kkv_document_parser_test',
    srcs=['KKVDocumentParserTest.cpp'],
    copts=['-fno-access-control'],
    data=['testdata/kkv_schema.json'],
    deps=[
        '//aios/storage/indexlib/document/kkv:kkv_document',
        '//aios/storage/indexlib/table/kkv_table:table',
        '//aios/unittest_framework'
    ]
)
