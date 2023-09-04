load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='NumberIndexMerger',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/index/inverted_index:InvertedIndexMerger',
        '//aios/storage/indexlib/index/inverted_index:OnDiskIndexIteratorCreator',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/pack:OnDiskPackIndexIterator'
    ]
)
