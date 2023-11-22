load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=[':__subpackages__'])
strict_cc_library(
    name='factory',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':FieldMetaFactory']
)
strict_cc_library(
    name='config',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=['//aios/storage/indexlib/index/field_meta/config']
)
strict_cc_library(name='Common', srcs=[])
strict_cc_library(
    name='FieldMetaFactory',
    deps=[
        ':Common', ':FieldMetaDiskIndexer', ':FieldMetaMemIndexer',
        ':FieldMetaMerger', ':FieldMetaReader',
        '//aios/storage/indexlib/index:IIndexFactory',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/index/field_meta/config:FieldMetaConfig'
    ],
    alwayslink=True
)
strict_cc_library(
    name='FieldMetaMemIndexer',
    deps=[
        ':Common', ':SourceFieldWriter', '//aios/autil:NoCopyable',
        '//aios/autil:log', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/document:DocumentBatch',
        '//aios/storage/indexlib/document/extractor:IDocumentInfoExtractorFactory',
        '//aios/storage/indexlib/document/extractor/plain:FieldMetaFieldInfoExtractor',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/common/field_format/field_meta:field_meta_convertor',
        '//aios/storage/indexlib/index/field_meta/config:FieldMetaConfig',
        '//aios/storage/indexlib/index/field_meta/meta:MetaFactory'
    ]
)
strict_cc_library(
    name='FieldMetaDiskIndexer',
    deps=[
        ':Common', ':SourceFieldIndexFactory', ':SourceFieldReader',
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/index:DiskIndexerParameter',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/field_meta/config:FieldMetaConfig',
        '//aios/storage/indexlib/index/field_meta/meta:MetaFactory',
        '//aios/storage/indexlib/index/field_meta/meta:MinMaxFieldMeta'
    ]
)
strict_cc_library(
    name='FieldMetaReader',
    deps=[
        ':Common', ':FieldMetaDiskIndexer', ':FieldMetaMemIndexer',
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/field_meta/meta:MetaFactory'
    ]
)
strict_cc_library(
    name='FieldMetaMerger',
    deps=[
        ':Common', ':FieldMetaDiskIndexer', ':SourceFieldConfigGenerator',
        ':SourceFieldIndexFactory', '//aios/autil:NoCopyable',
        '//aios/autil:env_util', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/field_meta/config',
        '//aios/storage/indexlib/index/field_meta/meta:MetaFactory'
    ]
)
strict_cc_library(
    name='ISourceFieldReader',
    srcs=[],
    deps=[
        ':Common', '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/autil:mem_pool_base', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/field_meta/config'
    ]
)
strict_cc_library(
    name='ISourceFieldWriter',
    srcs=[],
    deps=[
        ':Common', '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/autil:mem_pool_base', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index:DocMapDumpParams',
        '//aios/storage/indexlib/index/common/field_format/field_meta:field_meta_convertor'
    ]
)
strict_cc_library(
    name='CommonSourceFieldMerger',
    deps=[
        ':Common', ':ISourceFieldReader', '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/attribute:merger',
        '//aios/storage/indexlib/index/field_meta/config'
    ]
)
strict_cc_library(
    name='SingleValueSourceFieldMerger',
    srcs=[],
    deps=[
        ':CommonSourceFieldMerger',
        '//aios/storage/indexlib/index/attribute:merger'
    ]
)
strict_cc_library(
    name='MultiValueSourceFieldMerger',
    srcs=[],
    deps=[
        ':CommonSourceFieldMerger',
        '//aios/storage/indexlib/index/attribute:merger'
    ]
)
strict_cc_library(
    name='SourceFieldIndexFactory',
    deps=[
        ':Common', ':ISourceFieldReader', ':MultiValueSourceFieldMerger',
        ':SingleValueSourceFieldMerger', ':SourceFieldConfigGenerator',
        ':SourceFieldReader', '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/base:FieldTypeUtil',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/index/attribute/config',
        '//aios/storage/indexlib/index/field_meta/config'
    ]
)
strict_cc_library(
    name='SourceFieldReader',
    deps=[
        ':ISourceFieldReader', ':SourceFieldConfigGenerator',
        '//aios/storage/indexlib/index/attribute:factory',
        '//aios/storage/indexlib/index/attribute:indexer',
        '//aios/storage/indexlib/index/field_meta/config'
    ]
)
strict_cc_library(
    name='SourceFieldConfigGenerator',
    deps=[
        ':Common', '//aios/storage/indexlib/index/attribute:config',
        '//aios/storage/indexlib/index/field_meta/config'
    ]
)
strict_cc_library(
    name='SourceFieldWriter',
    deps=[
        ':ISourceFieldWriter', ':SourceFieldConfigGenerator',
        '//aios/storage/indexlib/document/extractor/plain:DocumentInfoExtractorFactory',
        '//aios/storage/indexlib/index/attribute:factory',
        '//aios/storage/indexlib/index/attribute:indexer',
        '//aios/storage/indexlib/index/field_meta/config'
    ]
)
strict_cc_library(
    name='SingleFieldMetaBuilder',
    deps=[
        ':FieldMetaMemIndexer',
        '//aios/storage/indexlib/document/normal:NormalDocument',
        '//aios/storage/indexlib/framework:TabletData'
    ]
)
strict_cc_library(
    name='FieldMetaBuildWorkItem',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':SingleFieldMetaBuilder',
        '//aios/storage/indexlib/index/common:BuildWorkItem'
    ]
)
