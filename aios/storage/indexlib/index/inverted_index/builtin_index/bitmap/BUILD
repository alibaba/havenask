load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='BitmapDiskIndexer',
    deps=[
        ':BitmapLeafReader', '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:DictionaryCreator',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:InMemDictionaryReader'
    ]
)
strict_cc_library(
    name='BitmapLeafReader',
    deps=[
        ':BitmapPostingExpandData', ':BitmapPostingWriter',
        '//aios/storage/indexlib/index/inverted_index:InvertedIndexSearchTracer',
        '//aios/storage/indexlib/index/inverted_index/format:TermMeta',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:DictionaryReader',
        '//aios/storage/indexlib/util:HashMap'
    ]
)
strict_cc_library(
    name='BitmapIndexReader',
    deps=[
        ':BitmapIndexWriter', ':BitmapLeafReader', ':BitmapPostingIterator',
        ':InMemBitmapIndexSegmentReader',
        '//aios/storage/indexlib/index/inverted_index:BuildingIndexReader',
        '//aios/storage/indexlib/index/inverted_index:InvertedIndexReader'
    ]
)
strict_cc_library(
    name='BitmapPostingMerger',
    deps=[
        ':BitmapPostingDecoder', ':MultiSegmentBitmapPostingWriter',
        '//aios/autil:log',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResource',
        '//aios/storage/indexlib/index:DocMapper',
        '//aios/storage/indexlib/index/inverted_index:PostingMerger'
    ]
)
strict_cc_library(
    name='MultiSegmentBitmapPostingWriter',
    deps=[
        ':BitmapPostingIterator', ':BitmapPostingWriter', '//aios/autil:log',
        '//aios/storage/indexlib/framework:SegmentMeta',
        '//aios/storage/indexlib/index:DocMapper',
        '//aios/storage/indexlib/index/inverted_index:MultiSegmentPostingIterator',
        '//aios/storage/indexlib/index/inverted_index:PostingMerger'
    ]
)
strict_cc_library(
    name='BitmapPostingWriter',
    deps=[
        '//aios/autil:memory', '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system:byte_slice_rw',
        '//aios/storage/indexlib/index/inverted_index:PostingWriter',
        '//aios/storage/indexlib/index/inverted_index:SegmentPostings',
        '//aios/storage/indexlib/index/inverted_index/format:TermMetaDumper',
        '//aios/storage/indexlib/util:ExpandableBitmap'
    ]
)
strict_cc_library(
    name='BitmapPostingDumper',
    deps=[
        ':BitmapPostingWriter',
        '//aios/storage/indexlib/index/inverted_index:PostingDumper'
    ]
)
strict_cc_library(
    name='BitmapPostingIterator',
    deps=['//aios/storage/indexlib/index/inverted_index:PositionIteratorTyped']
)
strict_cc_library(
    name='SingleBitmapPostingIterator',
    deps=[
        ':BitmapInDocPositionState', ':BitmapPostingExpandData',
        ':BitmapPostingWriter', ':InMemBitmapIndexDecoder',
        '//aios/storage/indexlib/index/common:error_code',
        '//aios/storage/indexlib/index/inverted_index:PostingWriter',
        '//aios/storage/indexlib/index/inverted_index:TermMatchData'
    ]
)
strict_cc_library(
    name='BitmapInDocPositionState',
    deps=[
        ':BitmapInDocPositionIterator',
        '//aios/storage/indexlib/util:object_pool'
    ]
)
strict_cc_library(
    name='BitmapInDocPositionIterator',
    srcs=[],
    deps=['//aios/storage/indexlib/index/inverted_index:InDocPositionState']
)
strict_cc_library(name='BitmapPostingExpandData', srcs=[])
strict_cc_library(name='InMemBitmapIndexDecoder', deps=[':BitmapPostingWriter'])
strict_cc_library(
    name='OnDiskBitmapIndexIterator',
    deps=[
        ':BitmapPostingDecoder', '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/inverted_index:OnDiskIndexIterator',
        '//aios/storage/indexlib/index/inverted_index/format:TermMetaLoader',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:CommonDiskTieredDictionaryReader'
    ]
)
strict_cc_library(
    name='BitmapPostingDecoder',
    deps=[
        '//aios/storage/indexlib/file_system:byte_slice_rw',
        '//aios/storage/indexlib/index/inverted_index/format:PostingDecoder'
    ]
)
strict_cc_library(
    name='InMemBitmapIndexSegmentReader',
    deps=[
        ':BitmapPostingWriter',
        '//aios/storage/indexlib/index/inverted_index:IndexSegmentReader',
        '//aios/storage/indexlib/index/inverted_index:InvertedIndexSearchTracer',
        '//aios/storage/indexlib/index/inverted_index:PostingIterator',
        '//aios/storage/indexlib/index/inverted_index:SegmentPosting',
        '//aios/storage/indexlib/util:HashMap'
    ]
)
strict_cc_library(
    name='BitmapIndexWriter',
    deps=[
        ':BitmapPostingWriter', ':InMemBitmapIndexSegmentReader',
        '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/document/normal:IndexDocument',
        '//aios/storage/indexlib/index/inverted_index:InvertedIndexUtil',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:TieredDictionaryWriter'
    ]
)
