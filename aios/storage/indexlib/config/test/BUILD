load('//aios/storage:defs.bzl', 'strict_cc_fast_test', 'strict_cc_library')
strict_cc_library(
    name='field_config_test_helper',
    testonly=True,
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':FieldConfigMaker']
)
strict_cc_library(
    name='FieldConfigMaker',
    testonly=True,
    copts=['-fno-access-control'],
    deps=['//aios/storage/indexlib/config:FieldConfig']
)
strict_cc_fast_test(
    name='config_unittest',
    srcs=[
        'BackgroundTaskConfigTest.cpp', 'BuildConfigTest.cpp',
        'BuildOptionConfigTest.cpp', 'CompressTypeOptionTest.cpp',
        'IndexTaskConfigTest.cpp', 'MergeConfigTest.cpp', 'MutableJsonTest.cpp',
        'OfflineConfigTest.cpp', 'OnlineConfigTest.cpp',
        'SingleFileCompressConfigTest.cpp', 'SortDescriptionTest.cpp',
        'TabletOptionsTest.cpp', 'TabletSchemaTest.cpp',
        'UnresolvedSchemaTest.cpp'
    ],
    copts=['-fno-access-control'],
    data=['testdata/legacy_schema_1.json'],
    deps=[
        '//aios/storage/indexlib/config',
        '//aios/storage/indexlib/config:CompressTypeOption',
        '//aios/storage/indexlib/config:SingleFileCompressConfig',
        '//aios/storage/indexlib/config:sort_description',
        '//aios/storage/indexlib/util:json_map',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
