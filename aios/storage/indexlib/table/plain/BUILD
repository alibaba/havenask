load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
indexlib_cc_library(
    name='PlainMemSegment',
    deps=[
        ':SegmentMetricsDumpItem', '//aios/autil:NoCopyable',
        '//aios/future_lite', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/document:DocumentBatch',
        '//aios/storage/indexlib/document:PlainDocument',
        '//aios/storage/indexlib/document/extractor/plain:DocumentInfoExtractorFactory',
        '//aios/storage/indexlib/framework:SegmentDumpItem',
        '//aios/storage/indexlib/index:BuildingIndexMemoryUseUpdater',
        '//aios/storage/indexlib/index:IndexFactoryCreator',
        '//aios/storage/indexlib/index:IndexerParameter',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/table/plain:PlainDumpItem',
        '//aios/storage/indexlib/util:simple_pool'
    ]
)
indexlib_cc_library(
    name='PlainDiskSegment',
    deps=[
        '//aios/autil:NoCopyable', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/framework:DiskSegment',
        '//aios/storage/indexlib/framework/cleaner:DropIndexCleaner',
        '//aios/storage/indexlib/index:IndexFactoryCreator',
        '//aios/storage/indexlib/index:IndexerParameter',
        '//aios/storage/indexlib/index:interface'
    ]
)
indexlib_cc_library(name='SingleShardMemSegment', deps=[':PlainMemSegment'])
indexlib_cc_library(
    name='SingleShardDiskSegment',
    deps=[':PlainDiskSegment', '//aios/storage/indexlib/base:PathUtil']
)
indexlib_cc_library(
    name='MultiShardDiskSegment',
    deps=[
        ':MultiShardSegmentMetrics', ':PlainDiskSegment',
        '//aios/storage/indexlib/base:PathUtil'
    ]
)
indexlib_cc_library(name='MultiShardSegmentMetrics', srcs=[], deps=[])
indexlib_cc_library(
    name='MultiShardMemSegment',
    deps=[
        ':MultiShardSegmentMetrics', ':PlainMemSegment',
        '//aios/storage/indexlib/base:PathUtil',
        '//aios/storage/indexlib/document/kv:kv_document',
        '//aios/storage/indexlib/index/common:ShardPartitioner'
    ]
)
indexlib_cc_library(
    name='SegmentMetricsDumpItem',
    deps=[
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/framework:SegmentDumpItem',
        '//aios/storage/indexlib/index:interface'
    ]
)
indexlib_cc_library(
    name='PlainDumpItem',
    deps=[
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/framework:SegmentDumpItem',
        '//aios/storage/indexlib/index:interface'
    ]
)
