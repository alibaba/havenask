load('//aios/storage:defs.bzl', 'strict_cc_fast_test', 'strict_cc_library')
strict_cc_library(
    name='SummaryMaker',
    testonly=True,
    copts=['-fno-access-control'],
    deps=[
        '//aios/autil:const_string_util', '//aios/autil:string_type',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/document/extractor/plain:DocumentInfoExtractorFactory',
        '//aios/storage/indexlib/document/normal:NormalDocument',
        '//aios/storage/indexlib/document/normal:SummaryFormatter',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system:interface',
        '//aios/storage/indexlib/index:IIndexFactory',
        '//aios/storage/indexlib/index:IndexFactoryCreator',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/attribute:factory',
        '//aios/storage/indexlib/index/deletionmap:factory',
        '//aios/storage/indexlib/index/operation_log:factory',
        '//aios/storage/indexlib/index/primary_key:factory',
        '//aios/storage/indexlib/index/summary:Common',
        '//aios/storage/indexlib/index/summary:SummaryMemIndexer',
        '//aios/storage/indexlib/index/summary:factory',
        '//aios/storage/indexlib/index/summary/config:SummaryIndexConfig'
    ]
)
strict_cc_fast_test(
    name='SummaryMemIndexerTest',
    srcs=['SummaryMemIndexerTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        '//aios/autil:NoCopyable', '//aios/autil:const_string_util',
        '//aios/autil:log', '//aios/autil:string_type',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/document/extractor/plain:DocumentInfoExtractorFactory',
        '//aios/storage/indexlib/document/normal:SummaryFormatter',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index:BuildingIndexMemoryUseUpdater',
        '//aios/storage/indexlib/index:IndexerParameter',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/summary:LocalDiskSummaryMemIndexer',
        '//aios/storage/indexlib/index/summary:SummaryDiskIndexer',
        '//aios/storage/indexlib/index/summary:SummaryMemIndexer',
        '//aios/storage/indexlib/index/summary/config:SummaryIndexConfig',
        '//aios/storage/indexlib/table/normal_table/test:NormalTabletSchemaMaker',
        '//aios/unittest_framework'
    ]
)
strict_cc_fast_test(
    name='SummaryMemReaderTest',
    srcs=['SummaryMemReaderTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        ':SummaryMaker', '//aios/autil:NoCopyable',
        '//aios/autil:const_string_util', '//aios/autil:log',
        '//aios/autil:string_type', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index:BuildingIndexMemoryUseUpdater',
        '//aios/storage/indexlib/index:IndexerParameter',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/summary:SummaryMemReader',
        '//aios/storage/indexlib/index/summary:SummaryMemReaderContainer',
        '//aios/storage/indexlib/index/summary/config:SummaryIndexConfig',
        '//aios/storage/indexlib/table/normal_table/test:NormalTabletSchemaMaker',
        '//aios/unittest_framework'
    ]
)
strict_cc_fast_test(
    name='LocalDiskSummaryDiskIndexerTest',
    srcs=['LocalDiskSummaryDiskIndexerTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        ':SummaryMaker', '//aios/autil:NoCopyable',
        '//aios/autil:const_string_util', '//aios/autil:log',
        '//aios/autil:string_type', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system/test:test-util',
        '//aios/storage/indexlib/index:IndexerParameter',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/summary:LocalDiskSummaryDiskIndexer',
        '//aios/storage/indexlib/table/normal_table/test:NormalTabletSchemaMaker',
        '//aios/unittest_framework'
    ]
)
strict_cc_fast_test(
    name='SummaryReaderTest',
    srcs=['SummaryReaderTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        ':SummaryMaker', '//aios/autil:NoCopyable',
        '//aios/autil:const_string_util', '//aios/autil:log',
        '//aios/autil:string_type', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system/test:test-util',
        '//aios/storage/indexlib/index:IndexerParameter',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/summary:LocalDiskSummaryDiskIndexer',
        '//aios/storage/indexlib/index/summary:SummaryReader',
        '//aios/storage/indexlib/table/normal_table/test:NormalTabletSchemaMaker',
        '//aios/unittest_framework'
    ]
)
strict_cc_fast_test(
    name='SummaryIndexFileTest',
    srcs=['SummaryIndexFileTest.cpp'],
    deps=[
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/summary:constants',
        '//aios/unittest_framework'
    ]
)
