load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
indexlib_cc_library(
    name='NumberIndexMerger',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/index/inverted_index:InvertedIndexMerger',
        '//aios/storage/indexlib/index/inverted_index:OnDiskIndexIteratorCreator',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/pack:OnDiskPackIndexIterator'
    ]
)
