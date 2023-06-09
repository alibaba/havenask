load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
indexlib_cc_library(
    name='OnDiskExpackIndexIterator',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/index/inverted_index/builtin_index/pack:OnDiskPackIndexIterator'
    ]
)
indexlib_cc_library(
    name='ExpackIndexMerger',
    srcs=[],
    deps=[
        ':OnDiskExpackIndexIterator',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/pack:PackIndexMerger'
    ]
)
