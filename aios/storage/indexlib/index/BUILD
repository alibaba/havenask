load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
indexlib_cc_library(
    name='interface',
    srcs=[],
    hdrs=[
        'IDiskIndexer.h', 'IIndexReader.h', 'IIndexer.h', 'IMemIndexer.h',
        'IShardRecordIterator.h'
    ],
    visibility=['//aios/storage:__subpackages__'],
    deps=[
        '//aios/autil:mem_pool_base', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/document:DocumentBatch',
        '//aios/storage/indexlib/document/extractor:IDocumentInfoExtractor'
    ]
)
indexlib_cc_library(
    name='IIndexReader', srcs=[], deps=['//aios/storage/indexlib/base:Status']
)
indexlib_cc_library(
    name='IIndexMerger', srcs=[], deps=['//aios/storage/indexlib/base:Status']
)
indexlib_cc_library(
    name='IIndexFactory',
    srcs=[],
    deps=[
        ':IIndexMerger', ':IIndexReader', ':IndexFactoryCreator',
        ':IndexerParameter', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/document:IIndexFieldsParser',
        '//aios/storage/indexlib/framework:MetricsManager',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/index:interface'
    ]
)
indexlib_cc_library(
    name='IndexFactoryCreator',
    hdrs=['IIndexFactory.h', 'IndexFactoryCreator.h'],
    deps=[
        '//aios/autil:lock', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/document:IIndexFieldsParser',
        '//aios/storage/indexlib/util:Singleton'
    ]
)
indexlib_cc_library(
    name='BuildingIndexMemoryUseUpdater',
    deps=['//aios/storage/indexlib/util/memory_control']
)
indexlib_cc_library(
    name='DocMapper',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/framework/index_task:task_execute'
    ]
)
indexlib_cc_library(
    name='IndexerParameter',
    srcs=[],
    deps=[
        ':IndexerDirectories', '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/config:SortDescription',
        '//aios/storage/indexlib/framework:MetricsManager'
    ]
)
indexlib_cc_library(
    name='IndexerDirectories',
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/file_system:interface'
    ]
)
indexlib_cc_library(
    name='DocMapDumpParams',
    srcs=[],
    deps=['//aios/storage/indexlib/framework:DumpParams']
)
