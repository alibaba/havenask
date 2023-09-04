load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=[':__subpackages__'])
strict_cc_library(
    name='factory',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib/index/inverted_index:__subpackages__'],
    deps=[':SectionAttributeIndexFactory']
)
strict_cc_library(
    name='InDocMultiSectionMeta',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/index/common/field_format/section_attribute:InDocSectionMeta',
        '//aios/storage/indexlib/index/common/field_format/section_attribute:MultiSectionMeta',
        '//aios/storage/indexlib/index/inverted_index:InDocSectionMeta',
        '//aios/storage/indexlib/index/inverted_index/config:PackageIndexConfig'
    ]
)
strict_cc_library(
    name='SectionAttributeReaderImpl',
    srcs=['SectionAttributeReaderImpl.cpp', 'SectionDataReader.cpp'],
    hdrs=['SectionAttributeReaderImpl.h', 'SectionDataReader.h'],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':InDocMultiSectionMeta', ':SectionAttributeMemIndexer',
        '//aios/autil:log', '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/index/attribute:reader',
        '//aios/storage/indexlib/index/common/field_format/section_attribute:SectionAttributeFormatter',
        '//aios/storage/indexlib/index/inverted_index:IInvertedDiskIndexer',
        '//aios/storage/indexlib/index/inverted_index:IInvertedMemIndexer',
        '//aios/storage/indexlib/index/inverted_index:SectionAttributeReader'
    ]
)
strict_cc_library(
    name='SectionAttributeMemIndexer',
    visibility=['//aios/storage/indexlib/index/inverted_index:__subpackages__'],
    deps=[
        '//aios/autil:log',
        '//aios/storage/indexlib/document/normal:IndexDocument',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/attribute:indexer',
        '//aios/storage/indexlib/index/inverted_index/config:PackageIndexConfig'
    ]
)
strict_cc_library(
    name='SectionAttributeMerger',
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/index/attribute:merger',
        '//aios/storage/indexlib/index/inverted_index:IInvertedDiskIndexer'
    ]
)
strict_cc_library(
    name='SectionAttributeIndexFactory',
    deps=[
        ':SectionAttributeMerger', '//aios/autil:log',
        '//aios/storage/indexlib/index/attribute:factory'
    ]
)
