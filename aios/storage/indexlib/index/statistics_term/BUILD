load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=[':__subpackages__'])
strict_cc_library(
    name='factory',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':StatisticsTermIndexFactory']
)
strict_cc_library(
    name='constants',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':Constant']
)
strict_cc_library(
    name='Constant',
    srcs=[],
    visibility=['//aios/storage/indexlib:__subpackages__']
)
strict_cc_library(
    name='StatisticsTermFormatter',
    deps=['//aios/autil:log', '//aios/storage/indexlib/index:interface']
)
strict_cc_library(
    name='StatisticsTermIndexConfig',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':Constant', '//aios/autil:log',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/config:IndexConfigDeserializeResource'
    ]
)
strict_cc_library(
    name='StatisticsTermMemIndexer',
    deps=[
        ':Constant', ':StatisticsTermFormatter', ':StatisticsTermIndexConfig',
        '//aios/autil:log', '//aios/storage/indexlib/document:DocumentBatch',
        '//aios/storage/indexlib/document/normal:IndexDocument',
        '//aios/storage/indexlib/document/normal:NormalDocument',
        '//aios/storage/indexlib/framework:SegmentMetrics',
        '//aios/storage/indexlib/index:BuildingIndexMemoryUseUpdater',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/util:simple_pool'
    ]
)
strict_cc_library(
    name='StatisticsTermLeafReader',
    deps=[
        ':Constant', ':StatisticsTermFormatter', ':StatisticsTermIndexConfig',
        '//aios/autil:log', '//aios/autil:string_helper',
        '//aios/storage/indexlib/file_system'
    ]
)
strict_cc_library(
    name='StatisticsTermDiskIndexer',
    deps=[
        ':StatisticsTermIndexConfig', ':StatisticsTermLeafReader',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index:interface'
    ]
)
strict_cc_library(
    name='StatisticsTermReader',
    deps=[
        ':StatisticsTermDiskIndexer', ':StatisticsTermIndexConfig',
        ':StatisticsTermLeafReader', '//aios/autil:result',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/index:IIndexReader'
    ]
)
strict_cc_library(
    name='StatisticsTermMerger',
    deps=[
        ':Constant', ':StatisticsTermDiskIndexer', ':StatisticsTermFormatter',
        ':StatisticsTermIndexConfig', ':StatisticsTermLeafReader',
        '//aios/autil:result', '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResourceManager',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/index:IIndexReader'
    ]
)
strict_cc_library(
    name='StatisticsTermIndexFactory',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':StatisticsTermDiskIndexer', ':StatisticsTermIndexConfig',
        ':StatisticsTermLeafReader', ':StatisticsTermMemIndexer',
        ':StatisticsTermMerger', ':StatisticsTermReader',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/index:DiskIndexerParameter',
        '//aios/storage/indexlib/index:IIndexFactory',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/index:IIndexReader'
    ],
    alwayslink=True
)
