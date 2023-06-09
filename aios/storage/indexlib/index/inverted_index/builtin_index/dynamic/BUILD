load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(
    default_visibility=[
        '//aios/storage/indexlib/index/inverted_index:__subpackages__'
    ]
)
indexlib_cc_library(
    name='DynamicSearchTreeNode',
    deps=[
        '//aios/autil:string_helper',
        '//aios/storage/indexlib/index/inverted_index/format:ComplexDocid'
    ]
)
indexlib_cc_library(
    name='NodeManager',
    deps=[
        ':DynamicSearchTreeNode', '//aios/autil:env_util',
        '//aios/storage/indexlib/util:EpochBasedReclaimManager',
        '//aios/storage/indexlib/util:simple_pool'
    ]
)
indexlib_cc_library(
    name='DynamicSearchTree',
    deps=[
        ':DynamicSearchTreeNode', ':NodeManager',
        '//aios/storage/indexlib/base:constants'
    ]
)
indexlib_cc_library(
    name='DynamicPostingWriter',
    deps=[
        ':DynamicSearchTree', ':NodeManager',
        '//aios/storage/indexlib/document/normal:ModifiedTokens',
        '//aios/storage/indexlib/index/inverted_index:PostingWriter',
        '//aios/storage/indexlib/index/inverted_index/format:TermMetaDumper'
    ]
)
indexlib_cc_library(
    name='DynamicIndexSegmentReader',
    deps=[
        ':DynamicPostingWriter', '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/index/inverted_index:IndexSegmentReader',
        '//aios/storage/indexlib/index/inverted_index:InvertedIndexUtil',
        '//aios/storage/indexlib/index/inverted_index:PostingIterator',
        '//aios/storage/indexlib/index/inverted_index:SegmentPosting',
        '//aios/storage/indexlib/util:HashMap'
    ]
)
indexlib_cc_library(
    name='DynamicMemIndexer',
    deps=[
        ':DynamicIndexSegmentReader',
        '//aios/storage/indexlib/document/normal:IndexDocument',
        '//aios/storage/indexlib/file_system'
    ]
)
indexlib_cc_library(name='DynamicIndexTermUpdater', deps=[':DynamicMemIndexer'])
indexlib_cc_library(
    name='DynamicPostingIterator',
    deps=[
        ':DynamicSearchTree',
        '//aios/storage/indexlib/index/inverted_index:PostingIterator',
        '//aios/storage/indexlib/index/inverted_index/format:TermMeta'
    ]
)
indexlib_cc_library(
    name='DynamicIndexReader',
    deps=[
        ':DynamicIndexTermUpdater', ':DynamicMemIndexer',
        ':DynamicPostingIterator', '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/inverted_index:BuildingIndexReader',
        '//aios/storage/indexlib/index/inverted_index:InvertedIndexMetrics'
    ]
)
