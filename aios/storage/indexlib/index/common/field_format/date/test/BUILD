package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
load('//aios/storage:defs.bzl', 'strict_cc_library')
strict_cc_library(
    name='DateTermMaker',
    deps=['//aios/storage/indexlib/index/common/field_format/date:DateTerm']
)
strict_cc_fast_test(
    name='date_format_test',
    srcs=[
        'DateFieldEncoderTest.cpp', 'DateQueryParserTest.cpp',
        'DateTermTest.cpp'
    ],
    copts=['-fno-access-control'],
    deps=[
        ':DateTermMaker',
        '//aios/storage/indexlib/index/common/field_format/date:DateFieldEncoder',
        '//aios/storage/indexlib/index/common/field_format/date:DateQueryParser',
        '//aios/storage/indexlib/table/normal_table/test:NormalTabletSchemaMaker',
        '//aios/unittest_framework'
    ]
)
