load('//aios/storage:defs.bzl', 'strict_cc_fast_test', 'strict_cc_library')
strict_cc_library(
    name='AttributeTestUtil',
    testonly=True,
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/autil:const_string_util', '//aios/autil:string_type',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/document/extractor/plain:DocumentInfoExtractorFactory',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system:interface',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/attribute:AttributeMemIndexer',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format',
        '//aios/storage/indexlib/table/normal_table/test:NormalTabletSchemaMaker',
        '//aios/storage/indexlib/util:Random', '//aios/unittest_framework'
    ]
)
strict_cc_library(
    name='IndexMemoryReclaimerTest',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/framework/mem_reclaimer:IIndexMemoryReclaimer'
    ]
)
strict_cc_fast_test(
    name='single_value_attribute_mem_indexer_test',
    srcs=['SingleValueAttributeMemIndexerTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        ':AttributeTestUtil', '//aios/autil:NoCopyable',
        '//aios/autil:const_string_util', '//aios/autil:log',
        '//aios/autil:string_type', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/document/extractor/plain:DocumentInfoExtractorFactory',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index:BuildingIndexMemoryUseUpdater',
        '//aios/storage/indexlib/index:IndexerParameter',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/attribute:AttributeDiskIndexer',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format'
    ]
)
strict_cc_fast_test(
    name='attribute_index_factory_test',
    srcs=['AttributeIndexFactoryTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        ':AttributeTestUtil', '//aios/autil:log',
        '//aios/storage/indexlib/index:IndexerParameter',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/attribute:AttributeIndexFactory',
        '//aios/storage/indexlib/index/attribute/merger:AttributeMerger'
    ]
)
strict_cc_fast_test(
    name='multi_value_attribute_mem_indexer_test',
    srcs=['MultiValueAttributeMemIndexerTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        ':AttributeTestUtil', '//aios/autil:NoCopyable',
        '//aios/autil:const_string_util', '//aios/autil:log',
        '//aios/autil:string_type', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/document/extractor/plain:DocumentInfoExtractorFactory',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index:BuildingIndexMemoryUseUpdater',
        '//aios/storage/indexlib/index:IndexerParameter',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/attribute:AttributeDiskIndexer',
        '//aios/storage/indexlib/index/common:AttributeValueTypeTraits',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format'
    ]
)
strict_cc_fast_test(
    name='MultiValueAttributeMemReaderTest',
    srcs=['MultiValueAttributeMemReaderTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        '//aios/autil:NoCopyable', '//aios/autil:const_string_util',
        '//aios/autil:log', '//aios/autil:string_type',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/document/extractor/plain:DocumentInfoExtractorFactory',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index:IndexerParameter',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/attribute:MultiValueAttributeMemReader',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format',
        '//aios/unittest_framework'
    ]
)
strict_cc_fast_test(
    name='SingleValueAttributeMemReaderTest',
    srcs=['SingleValueAttributeMemReaderTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        '//aios/autil:const_string_util', '//aios/autil:log',
        '//aios/autil:string_type', '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index:DocMapDumpParams',
        '//aios/storage/indexlib/index/attribute:SingleValueAttributeMemReader',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format',
        '//aios/unittest_framework'
    ]
)
strict_cc_fast_test(
    name='SingleValueAttributeDiskIndexerTest',
    srcs=['SingleValueAttributeDiskIndexerTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        ':AttributeTestUtil', '//aios/autil:const_string_util',
        '//aios/autil:log', '//aios/autil:string_type',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/attribute:AttributeDiskIndexer',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format'
    ]
)
strict_cc_fast_test(
    name='MultiValueAttributeCompressOffsetReaderTest',
    srcs=['MultiValueAttributeCompressOffsetReaderTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        ':AttributeTestUtil', '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/attribute:MultiValueAttributeCompressOffsetReader',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format',
        '//aios/unittest_framework'
    ]
)
strict_cc_fast_test(
    name='MultiValueAttributeUnCompressOffsetReaderTest',
    srcs=['MultiValueAttributeUnCompressOffsetReaderTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        ':AttributeTestUtil', '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/attribute:MultiValueAttributeUnCompressOffsetReader',
        '//aios/unittest_framework'
    ]
)
strict_cc_fast_test(
    name='MultiValueAttributeOffsetReaderTest',
    srcs=['MultiValueAttributeOffsetReaderTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        ':AttributeTestUtil', '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/attribute:MultiValueAttributeOffsetReader',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format',
        '//aios/unittest_framework'
    ]
)
strict_cc_fast_test(
    name='AttributeDataInfoTest',
    srcs=['AttributeDataInfoTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        '//aios/storage/indexlib/index/attribute:AttributeDataInfo',
        '//aios/unittest_framework'
    ]
)
strict_cc_fast_test(
    name='MultiValueAttributeDefragSliceArrayTest',
    srcs=['MultiValueAttributeDefragSliceArrayTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        ':AttributeTestUtil', ':IndexMemoryReclaimerTest',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/attribute:AttributeDiskIndexer',
        '//aios/storage/indexlib/index/attribute:AttributeMemIndexer',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format',
        '//aios/unittest_framework'
    ]
)
strict_cc_fast_test(
    name='MultiValueAttributeDiskIndexerTest',
    srcs=['MultiValueAttributeDiskIndexerTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        ':AttributeTestUtil', ':IndexMemoryReclaimerTest',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/attribute:AttributeDiskIndexer',
        '//aios/storage/indexlib/index/attribute:AttributeMemIndexer',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format',
        '//aios/unittest_framework'
    ]
)
strict_cc_fast_test(
    name='AttributeIteratorTypedTest',
    srcs=['AttributeIteratorTypedTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        '//aios/autil:const_string_util', '//aios/autil:string_type',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system/test:test-util',
        '//aios/storage/indexlib/index:DocMapDumpParams',
        '//aios/storage/indexlib/index/attribute:AttributeIteratorTyped',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format',
        '//aios/unittest_framework'
    ]
)
strict_cc_fast_test(
    name='AttributeIndexFileTest',
    srcs=['AttributeIndexFileTest.cpp'],
    deps=[
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/attribute:constants',
        '//aios/unittest_framework'
    ]
)
