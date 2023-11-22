load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=[':__subpackages__'])
strict_cc_library(
    name='factory',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':KKVIndexFactory']
)
strict_cc_library(
    name='reader',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=['//aios/storage/indexlib/index/kkv/search:kkv_index_reader']
)
strict_cc_library(
    name='merger',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=['//aios/storage/indexlib/index/kkv/merge:kkv_index_merger']
)
strict_cc_library(
    name='constants',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':Constant']
)
strict_cc_library(
    name='KKVIndexFactory',
    deps=[
        ':KKVShardRecordIterator', '//aios/autil:log',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/index:DiskIndexerParameter',
        '//aios/storage/indexlib/index:IIndexFactory',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/index:IIndexReader',
        '//aios/storage/indexlib/index/kkv/building:kkv_mem_indexer',
        '//aios/storage/indexlib/index/kkv/built:kkv_disk_indexer',
        '//aios/storage/indexlib/index/kkv/common:KKVSegmentStatistics',
        '//aios/storage/indexlib/index/kkv/common:Trait',
        '//aios/storage/indexlib/index/kkv/config',
        '//aios/storage/indexlib/index/kkv/merge:kkv_index_merger',
        '//aios/storage/indexlib/index/kkv/search:kkv_index_reader'
    ],
    alwayslink=True
)
strict_cc_library(name='Constant', srcs=[])
strict_cc_library(name='Types', srcs=[])
strict_cc_library(
    name='KKVShardRecordIterator',
    deps=[
        ':merger', '//aios/storage/indexlib/index:IShardRecordIterator',
        '//aios/storage/indexlib/index/kv:reader'
    ]
)
