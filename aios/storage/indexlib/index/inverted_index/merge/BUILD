load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
indexlib_cc_library(name='SimpleDocMapper', srcs=[])
indexlib_cc_library(
    name='SimpleInvertedIndexMerger',
    deps=[
        ':SimpleDocMapper', '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:SegmentMeta',
        '//aios/storage/indexlib/index/inverted_index:PostingMergerImpl',
        '//aios/storage/indexlib/index/inverted_index:SegmentTermInfoQueue',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:BitmapPostingMerger',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/pack:OnDiskPackIndexIterator'
    ]
)
