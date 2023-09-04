load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=[':__subpackages__'])
strict_cc_library(
    name='kkv_disk_iterator',
    srcs=[],
    hdrs=[
        'BuiltSKeyIterator.h', 'KKVBuiltSegmentIterator.h',
        'KKVBuiltSegmentIteratorBase.h', 'KKVBuiltSegmentIteratorFactory.h',
        'KKVBuiltSegmentIteratorOption.h', 'KKVBuiltValueFetcher.h',
        'KKVBuiltValueReader.h'
    ],
    visibility=['//aios/storage/indexlib/index/kkv:__subpackages__'],
    deps=[
        ':kkv_disk_reader', '//aios/autil:log', '//aios/autil:time',
        '//aios/storage/indexlib/base:NoExceptionWrapper',
        '//aios/storage/indexlib/index/kkv/common:ChunkReader',
        '//aios/storage/indexlib/index/kkv/common:InlineOnDiskSKeyNode',
        '//aios/storage/indexlib/index/kkv/common:KKVIndexFormat',
        '//aios/storage/indexlib/index/kkv/common:KKVResultBuffer',
        '//aios/storage/indexlib/index/kkv/common:KKVSegmentIteratorBase',
        '//aios/storage/indexlib/index/kkv/common:NormalOnDiskSKeyNode',
        '//aios/storage/indexlib/index/kkv/common:SKeyIteratorBase',
        '//aios/storage/indexlib/index/kkv/common:SKeySearchContext',
        '//aios/storage/indexlib/index/kkv/config'
    ]
)
strict_cc_library(
    name='kkv_disk_reader',
    srcs=[],
    hdrs=['KKVBuiltSegmentReader.h'],
    visibility=['//aios/storage/indexlib/index/kkv:__subpackages__'],
    deps=[
        '//aios/autil:log', '//aios/autil:time',
        '//aios/storage/indexlib/base:NoExceptionWrapper',
        '//aios/storage/indexlib/index/kkv/config'
    ]
)
strict_cc_library(
    name='kkv_disk_indexer',
    srcs=[],
    hdrs=['KKVDiskIndexer.h'],
    visibility=['//aios/storage/indexlib/index/kkv:__subpackages__'],
    deps=[
        ':kkv_disk_iterator', ':kkv_disk_reader', '//aios/autil:log',
        '//aios/storage/indexlib/index:interface'
    ]
)
