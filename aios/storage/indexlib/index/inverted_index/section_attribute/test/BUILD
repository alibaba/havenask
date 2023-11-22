load('//aios/storage:defs.bzl', 'strict_cc_fast_test', 'strict_cc_library')
strict_cc_library(
    name='SectionAttributeTestUtil',
    testonly=True,
    visibility=['//aios/storage/indexlib/index/inverted_index:__subpackages__'],
    deps=['//aios/storage/indexlib/index/attribute/test:AttributeTestUtil']
)
strict_cc_fast_test(
    name='SectionAttributeReaderTest',
    srcs=['SectionAttributeReaderTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        ':SectionAttributeTestUtil', '//aios/autil:const_string_util',
        '//aios/autil:string_helper', '//aios/autil:string_type',
        '//aios/storage/indexlib/base:PathUtil',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/document:DocumentBatch',
        '//aios/storage/indexlib/document/extractor/plain:DocumentInfoExtractorFactory',
        '//aios/storage/indexlib/document/normal:NormalDocument',
        '//aios/storage/indexlib/document/normal/rewriter:SectionAttributeRewriter',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/framework/mock:FakeDiskSegment',
        '//aios/storage/indexlib/index:DiskIndexerParameter',
        '//aios/storage/indexlib/index/attribute/test:AttributeTestUtil',
        '//aios/storage/indexlib/index/inverted_index:InvertedDiskIndexer',
        '//aios/storage/indexlib/index/inverted_index/section_attribute:SectionAttributeReaderImpl',
        '//aios/unittest_framework'
    ]
)
