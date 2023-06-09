load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
indexlib_cc_library(
    name='InDocMultiSectionMeta',
    deps=[
        '//aios/storage/indexlib/index/common/field_format/section_attribute:InDocSectionMeta',
        '//aios/storage/indexlib/index/common/field_format/section_attribute:MultiSectionMeta',
        '//aios/storage/indexlib/index/inverted_index:InDocSectionMeta',
        '//aios/storage/indexlib/index/inverted_index/config:PackageIndexConfig'
    ]
)
indexlib_cc_library(
    name='SectionAttributeReaderImpl',
    srcs=['SectionAttributeReaderImpl.cpp', 'SectionDataReader.cpp'],
    hdrs=['SectionAttributeReaderImpl.h', 'SectionDataReader.h'],
    deps=[
        ':InDocMultiSectionMeta', '//aios/autil:log',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/index/attribute:reader',
        '//aios/storage/indexlib/index/common/field_format/section_attribute:SectionAttributeFormatter',
        '//aios/storage/indexlib/index/inverted_index:IInvertedDiskIndexer',
        '//aios/storage/indexlib/index/inverted_index:IInvertedMemIndexer',
        '//aios/storage/indexlib/index/inverted_index:SectionAttributeMemIndexer',
        '//aios/storage/indexlib/index/inverted_index:SectionAttributeReader'
    ]
)
