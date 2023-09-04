load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='AdaptiveBitmapIndexWriter',
    deps=[
        ':AdaptiveBitmapTrigger',
        '//aios/storage/indexlib/index/inverted_index:PostingIterator',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:BitmapPostingWriter',
        '//aios/storage/indexlib/index/inverted_index/format:TermMeta',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:TieredDictionaryWriter'
    ]
)
strict_cc_library(
    name='MultiAdaptiveBitmapIndexWriter',
    srcs=[],
    deps=[
        ':AdaptiveBitmapIndexWriter', ':AdaptiveBitmapTrigger',
        '//aios/storage/indexlib/framework:SegmentMeta',
        '//aios/storage/indexlib/index/inverted_index:MultiSegmentPostingIterator',
        '//aios/storage/indexlib/index/inverted_index:PostingMerger'
    ]
)
strict_cc_library(name='AdaptiveBitmapTrigger', srcs=[])
strict_cc_library(
    name='PercentAdaptiveBitmapTrigger',
    srcs=[],
    deps=[
        ':AdaptiveBitmapTrigger',
        '//aios/storage/indexlib/index/inverted_index:PostingMerger',
        '//aios/storage/indexlib/util:MathUtil'
    ]
)
strict_cc_library(
    name='DfAdaptiveBitmapTrigger',
    srcs=[],
    deps=[
        ':AdaptiveBitmapTrigger',
        '//aios/storage/indexlib/index/inverted_index:PostingMerger'
    ]
)
strict_cc_library(
    name='IndexSizeAdaptiveBitmapTrigger',
    srcs=[],
    deps=[
        ':AdaptiveBitmapTrigger', '//aios/storage/indexlib/index:DocMapper',
        '//aios/storage/indexlib/index/inverted_index:PostingMerger'
    ]
)
strict_cc_library(
    name='AdaptiveBitmapIndexWriterCreator',
    deps=[
        ':AdaptiveBitmapTriggerCreator', ':MultiAdaptiveBitmapIndexWriter',
        '//aios/storage/indexlib/index/inverted_index:PostingMerger'
    ]
)
strict_cc_library(
    name='AdaptiveBitmapTriggerCreator',
    deps=[
        ':AdaptiveBitmapTrigger', ':DfAdaptiveBitmapTrigger',
        ':IndexSizeAdaptiveBitmapTrigger', ':PercentAdaptiveBitmapTrigger',
        '//aios/storage/indexlib/index/inverted_index:PostingMerger'
    ]
)
