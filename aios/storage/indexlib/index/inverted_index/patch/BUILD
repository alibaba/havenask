load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='InvertedIndexPatchWriter',
    srcs=[
        'IInvertedIndexSegmentUpdater.cpp', 'InvertedIndexPatchWriter.cpp',
        'InvertedIndexSegmentUpdater.cpp',
        'MultiShardInvertedIndexSegmentUpdater.cpp'
    ],
    hdrs=[
        'IInvertedIndexSegmentUpdater.h', 'InvertedIndexPatchWriter.h',
        'InvertedIndexSegmentUpdater.h',
        'MultiShardInvertedIndexSegmentUpdater.h'
    ],
    deps=[
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/document/normal:ModifiedTokens',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system/file:interface',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/index/common/patch:PatchFileInfos',
        '//aios/storage/indexlib/index/common/patch:PatchWriter',
        '//aios/storage/indexlib/index/inverted_index/config',
        '//aios/storage/indexlib/index/inverted_index/format:ComplexDocid',
        '//aios/storage/indexlib/index/inverted_index/format:PatchFormat',
        '//aios/storage/indexlib/index/inverted_index/format:ShardingIndexHasher',
        '//aios/storage/indexlib/util:Algorithm',
        '//aios/storage/indexlib/util:simple_pool'
    ]
)
strict_cc_library(
    name='InvertedIndexPatchFileFinder',
    srcs=[],
    deps=['//aios/storage/indexlib/index/common/patch:SrcDestPatchFileFinder']
)
strict_cc_library(
    name='IndexUpdateTermIterator',
    srcs=[],
    deps=['//aios/storage/indexlib/index/inverted_index/format:ComplexDocid']
)
strict_cc_library(
    name='IndexPatchFileReader',
    deps=[
        '//aios/storage/indexlib/document/normal:ModifiedTokens',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/common:DictKeyInfo',
        '//aios/storage/indexlib/index/inverted_index/format:ComplexDocid',
        '//aios/storage/indexlib/index/inverted_index/format:PatchFormat'
    ]
)
strict_cc_library(
    name='SingleTermIndexSegmentPatchIterator',
    deps=[
        ':IndexPatchFileReader', ':IndexUpdateTermIterator',
        '//aios/storage/indexlib/index/inverted_index/config:InvertedIndexConfig'
    ]
)
strict_cc_library(
    name='SingleFieldIndexSegmentPatchIterator',
    deps=[
        ':IndexPatchFileReader', ':IndexUpdateTermIterator',
        ':SingleTermIndexSegmentPatchIterator'
    ]
)
strict_cc_library(
    name='IInvertedIndexPatchIterator',
    srcs=[],
    deps=['//aios/autil:NoCopyable']
)
strict_cc_library(
    name='SingleFieldInvertedIndexPatchIterator',
    deps=[
        ':IInvertedIndexPatchIterator', ':InvertedIndexPatchFileFinder',
        ':SingleFieldIndexSegmentPatchIterator', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/config:interface',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/index/common/patch:PatchFileInfos',
        '//aios/storage/indexlib/index/inverted_index/config'
    ]
)
strict_cc_library(
    name='MultiFieldInvertedIndexPatchIterator',
    deps=[
        ':SingleFieldInvertedIndexPatchIterator', '//aios/autil:log',
        '//aios/storage/indexlib/config:interface'
    ]
)
strict_cc_library(
    name='InvertedIndexPatchIteratorCreator',
    deps=[
        ':MultiFieldInvertedIndexPatchIterator',
        ':SingleFieldInvertedIndexPatchIterator', '//aios/autil:NoCopyable',
        '//aios/storage/indexlib/config:interface',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/index/inverted_index/config'
    ]
)
strict_cc_library(
    name='InvertedIndexPatchMerger',
    deps=[
        ':SingleFieldIndexSegmentPatchIterator', '//aios/autil:log',
        '//aios/storage/indexlib/file_system/file:interface',
        '//aios/storage/indexlib/index/common/patch:PatchMerger',
        '//aios/storage/indexlib/index/inverted_index/config',
        '//aios/storage/indexlib/util:Algorithm'
    ]
)
strict_cc_library(
    name='InvertedIndexDedupPatchFileMerger',
    deps=[
        ':InvertedIndexPatchFileFinder', ':InvertedIndexPatchMerger',
        '//aios/autil:log',
        '//aios/storage/indexlib/file_system/file:interface',
        '//aios/storage/indexlib/index/common/patch:PatchFileMerger',
        '//aios/storage/indexlib/index/inverted_index/config'
    ]
)
