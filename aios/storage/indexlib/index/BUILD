load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='interface',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage:__subpackages__'],
    deps=[':IDiskIndexer', ':IIndexReader', ':IMemIndexer']
)
strict_cc_library(name='IIndexer', srcs=[])
strict_cc_library(
    name='IMemIndexer',
    srcs=[],
    deps=[
        ':IIndexer', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/document:interface'
    ]
)
strict_cc_library(
    name='IDiskIndexer',
    srcs=[],
    deps=[':IIndexer', '//aios/storage/indexlib/base:Status']
)
strict_cc_library(
    name='IIndexReader', srcs=[], deps=['//aios/storage/indexlib/base:Status']
)
strict_cc_library(
    name='IIndexMerger',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types'
    ]
)
strict_cc_library(name='IShardRecordIterator', srcs=[])
strict_cc_library(
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
strict_cc_library(
    name='IndexFactoryCreator',
    hdrs=['IIndexFactory.h', 'IndexFactoryCreator.h'],
    deps=[
        '//aios/autil:lock', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/document:IIndexFieldsParser',
        '//aios/storage/indexlib/util:Singleton'
    ]
)
strict_cc_library(
    name='BuildingIndexMemoryUseUpdater',
    deps=['//aios/storage/indexlib/util/memory_control']
)
strict_cc_library(
    name='DocMapper',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/framework/index_task:task_execute'
    ]
)
strict_cc_library(
    name='IndexerParameter',
    srcs=[],
    deps=[
        ':IndexerDirectories', '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/config:SortDescription',
        '//aios/storage/indexlib/framework:MetricsManager'
    ]
)
strict_cc_library(
    name='IndexerDirectories',
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/file_system:interface'
    ]
)
strict_cc_library(
    name='DocMapDumpParams',
    srcs=[],
    deps=['//aios/storage/indexlib/framework:DumpParams']
)
