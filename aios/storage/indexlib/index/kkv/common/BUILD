load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(
    default_visibility=['//aios/storage/indexlib/index/kkv:__subpackages__']
)
indexlib_cc_library(name='ChunkDefine', srcs=[])
indexlib_cc_library(name='OnDiskPKeyOffset', srcs=[])
indexlib_cc_library(name='InlineOnDiskSKeyNode', srcs=[])
indexlib_cc_library(name='NormalOnDiskSKeyNode', srcs=[])
indexlib_cc_library(
    name='SKeyListInfo',
    srcs=[],
    deps=['//aios/storage/indexlib/index/kkv:constants']
)
indexlib_cc_library(
    name='KKVDoc',
    srcs=[],
    deps=[':ChunkDefine', '//aios/storage/indexlib/index/kkv:constants']
)
indexlib_cc_library(
    name='SKeySearchContext', srcs=[], deps=['//aios/autil:mem_pool_base']
)
indexlib_cc_library(
    name='KKVDocs',
    srcs=[],
    deps=[
        ':KKVDoc', '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/util:PoolUtil',
        '//aios/storage/indexlib/util:PooledUniquePtr'
    ]
)
indexlib_cc_library(
    name='KKVResultBuffer',
    srcs=[],
    deps=[':KKVDocs', '//aios/autil:mem_pool_base', '//aios/autil:span']
)
indexlib_cc_library(name='SKeyIteratorBase', srcs=[])
indexlib_cc_library(
    name='KKVSegmentIteratorBase',
    srcs=[],
    hdrs=['KKVSegmentIteratorBase.h'],
    visibility=['//aios/storage/indexlib/index/kkv:__subpackages__'],
    deps=[':KKVTTLHelper', '//aios/storage/indexlib/index/kkv/config']
)
indexlib_cc_library(
    name='ChunkWriter',
    deps=[
        ':ChunkDefine', '//aios/autil:span',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/kkv:constants'
    ]
)
indexlib_cc_library(
    name='KKVIndexFormat',
    deps=[
        '//aios/autil:json', '//aios/storage/indexlib/file_system:interface',
        '//aios/storage/indexlib/index/kkv:constants'
    ]
)
indexlib_cc_library(name='KKVRecordFilter', srcs=[])
indexlib_cc_library(
    name='KKVTimestampHelper', srcs=[], deps=['//aios/autil:time']
)
indexlib_cc_library(
    name='KKVTTLHelper',
    srcs=[],
    deps=['//aios/storage/indexlib/index/kkv:constants']
)
indexlib_cc_library(
    name='KKVSegmentStatistics',
    deps=[
        '//aios/autil:log', '//aios/autil:time',
        '//aios/storage/indexlib/base:NoExceptionWrapper',
        '//aios/storage/indexlib/framework:SegmentMetrics'
    ]
)
indexlib_cc_library(
    name='KKVMetricsCollector',
    srcs=[],
    deps=['//aios/storage/indexlib/index/kv:metrics']
)
indexlib_cc_library(
    name='ChunkReader',
    deps=[
        ':ChunkDefine', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/util:PooledUniquePtr'
    ]
)
indexlib_cc_library(
    name='Trait',
    srcs=[],
    deps=[':InlineOnDiskSKeyNode', ':NormalOnDiskSKeyNode']
)
