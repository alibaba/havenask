load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=[':__subpackages__'])
strict_cc_library(
    name='factory',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':PackAttributeIndexFactory']
)
strict_cc_library(
    name='config',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':PackAttributeConfig']
)
strict_cc_library(
    name='reader',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':PackAttributeReader']
)
strict_cc_library(
    name='constants',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':Common']
)
strict_cc_library(
    name='PackAttributeConfig',
    deps=[':Common', '//aios/storage/indexlib/index/attribute:config']
)
strict_cc_library(
    name='PackAttributeIndexFactory',
    deps=[
        ':MultiSlicePackAttributeDiskIndexer', ':PackAttributeConfig',
        ':PackAttributeDiskIndexer', ':PackAttributeIndexMerger',
        ':PackAttributeMemIndexer', ':PackAttributeReader', '//aios/autil:log',
        '//aios/storage/indexlib/framework:MetricsManager',
        '//aios/storage/indexlib/index:IIndexFactory',
        '//aios/storage/indexlib/index:IIndexMerger'
    ],
    alwayslink=True
)
strict_cc_library(name='Common', srcs=[])
strict_cc_library(
    name='MultiSlicePackAttributeDiskIndexer',
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/attribute:indexer'
    ]
)
strict_cc_library(
    name='PackAttributeDiskIndexer',
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/attribute:indexer'
    ]
)
strict_cc_library(
    name='PackAttributeMemIndexer',
    deps=[
        ':Common', ':PackAttributeConfig', ':PackAttributeIndexFields',
        '//aios/autil:log', '//aios/storage/indexlib/index/attribute:indexer'
    ]
)
strict_cc_library(
    name='PackAttributeIterator',
    srcs=[],
    hdrs=['PackAttributeIterator.h', 'PackAttributeIteratorTyped.h'],
    deps=[
        '//aios/storage/indexlib/index/attribute:reader',
        '//aios/storage/indexlib/index/common/field_format/pack_attribute'
    ]
)
strict_cc_library(
    name='PackAttributeReader',
    deps=[
        ':PackAttributeIterator', ':PackAttributeMetrics', '//aios/autil:log',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/attribute:reader'
    ]
)
strict_cc_library(
    name='PackAttributeMetrics',
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/index/attribute:reader',
        '//aios/storage/indexlib/util/counter'
    ]
)
strict_cc_library(
    name='PackAttributeIndexMerger',
    deps=['//aios/autil:log', '//aios/storage/indexlib/index/attribute:merger']
)
strict_cc_library(
    name='PackAttributeIndexFields',
    deps=[
        ':Common', '//aios/autil:log', '//aios/storage/indexlib/base:Constant',
        '//aios/storage/indexlib/document:IIndexFields',
        '//aios/storage/indexlib/util:PooledContainer'
    ]
)
