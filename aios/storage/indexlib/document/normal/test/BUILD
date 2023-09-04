load('//aios/storage:defs.bzl', 'strict_cc_fast_test', 'strict_cc_library')
strict_cc_library(
    name='normal_parser_test_helper',
    testonly=True,
    srcs=['SimpleTokenizer.cpp', 'TokenizeHelper.cpp'],
    hdrs=['SimpleTokenizer.h', 'TokenizeHelper.h'],
    copts=['-fno-access-control'],
    deps=[
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/document/normal:NormalExtendDocument',
        '//aios/storage/indexlib/document/normal/tokenize:AnalyzerToken',
        '//aios/storage/indexlib/document/normal/tokenize:TokenizeDocument',
        '//aios/storage/indexlib/index/primary_key:constants'
    ]
)
strict_cc_fast_test(
    name='normal_document_parser_test',
    srcs=['NormalDocumentParserTest.cpp', 'NullFieldAppenderTest.cpp'],
    copts=['-fno-access-control'],
    data=[
        'testdata/document_parser_simple_schema.json',
        'testdata/schema/document_parser_simple_schema.json'
    ],
    deps=[
        ':normal_parser_test_helper',
        '//aios/storage/indexlib/document/normal:NormalDocumentParser',
        '//aios/storage/indexlib/document/normal:NullFieldAppender',
        '//aios/storage/indexlib/document/raw_document:DefaultRawDocument',
        '//aios/storage/indexlib/framework:tablet_schema_loader',
        '//aios/storage/indexlib/table/normal_table:table',
        '//aios/storage/indexlib/table/normal_table/test:NormalTabletSchemaMaker',
        '//aios/unittest_framework'
    ]
)
