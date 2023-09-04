load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='OnDiskExpackIndexIterator',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/index/inverted_index/builtin_index/pack:OnDiskPackIndexIterator'
    ]
)
strict_cc_library(
    name='ExpackIndexMerger',
    srcs=[],
    deps=[
        ':OnDiskExpackIndexIterator',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/pack:PackIndexMerger'
    ]
)
