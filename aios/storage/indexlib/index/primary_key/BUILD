load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=[':__subpackages__'])
indexlib_cc_library(
    name='factory',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':PrimaryKeyIndexFactory']
)
indexlib_cc_library(
    name='reader',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':primary_key_reader']
)
indexlib_cc_library(
    name='constants',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':Common', ':Constant', ':Types']
)
indexlib_cc_library(
    name='util',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':PrimaryKeyHashConvertor']
)
indexlib_cc_library(
    name='PrimaryKeyIndexFactory',
    deps=[
        '//aios/storage/indexlib/index:IIndexFactory',
        '//aios/storage/indexlib/index/primary_key:primary_key_indexer',
        '//aios/storage/indexlib/index/primary_key:primary_key_reader',
        '//aios/storage/indexlib/index/primary_key/merger:PrimaryKeyMerger'
    ],
    alwayslink=True
)
indexlib_cc_library(
    name='PrimaryKeyIndexReader',
    srcs=[],
    visibility=['//visibility:public'],
    deps=[
        '//aios/autil:multi_value', '//aios/autil:string_type',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/index/inverted_index:InvertedIndexReader',
        '//aios/storage/indexlib/index/primary_key:Types'
    ]
)
indexlib_cc_library(
    name='primary_key_indexer',
    srcs=[],
    hdrs=[
        'BlockArrayPrimaryKeyDiskIndexer.h',
        'BlockArrayPrimaryKeyLeafIterator.h', 'BlockPrimaryKeyFileWriter.h',
        'HashPrimaryKeyFileWriter.h', 'HashTablePrimaryKeyDiskIndexer.h',
        'HashTablePrimaryKeyLeafIterator.h', 'PrimaryKeyDiskIndexer.h',
        'PrimaryKeyDiskIndexerTyped.h', 'PrimaryKeyFileWriter.h',
        'PrimaryKeyFileWriterCreator.h', 'PrimaryKeyHashTable.h',
        'PrimaryKeyLeafIterator.h', 'PrimaryKeyPair.h', 'PrimaryKeyWriter.h',
        'SortArrayPrimaryKeyDiskIndexer.h', 'SortArrayPrimaryKeyLeafIterator.h',
        'SortedPrimaryKeyFileWriter.h'
    ],
    deps=[
        '//aios/autil:NoCopyable', '//aios/autil:bloom_filter',
        '//aios/autil:log', '//aios/future_lite',
        '//aios/storage/indexlib/base:NoExceptionWrapper',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/document/extractor:IDocumentInfoExtractorFactory',
        '//aios/storage/indexlib/document/extractor/plain:DocumentInfoExtractorFactory',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index:BuildingIndexMemoryUseUpdater',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/attribute:factory',
        '//aios/storage/indexlib/index/attribute:indexer',
        '//aios/storage/indexlib/index/common/block_array:reader',
        '//aios/storage/indexlib/index/common/block_array:writer',
        '//aios/storage/indexlib/util:HashMap'
    ]
)
indexlib_cc_library(
    name='InMemPrimaryKeySegmentReaderTyped',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/index/inverted_index:IndexSegmentReader',
        '//aios/storage/indexlib/util:HashMap'
    ]
)
indexlib_cc_library(
    name='PrimaryKeyBuildingIndexReader',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/util:HashMap'
    ]
)
indexlib_cc_library(
    name='PrimaryKeyInDocPositionState',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/index/inverted_index:InDocPositionState',
        '//aios/storage/indexlib/index/inverted_index:Types'
    ]
)
indexlib_cc_library(
    name='PrimaryKeyPostingIterator',
    deps=[
        ':InMemPrimaryKeySegmentReaderTyped', ':PrimaryKeyInDocPositionState',
        '//aios/storage/indexlib/index/inverted_index:PostingIterator',
        '//aios/storage/indexlib/index/inverted_index:TermMatchData',
        '//aios/storage/indexlib/index/inverted_index/format:TermMeta'
    ]
)
indexlib_cc_library(
    name='SegmentDataAdapter',
    deps=[
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:SegmentInfo'
    ]
)
indexlib_cc_library(
    name='primary_key_reader_interface',
    srcs=[],
    hdrs=[
        'IPrimaryKeyIterator.h', 'PrimaryKeyIterator.h', 'PrimaryKeyLoadPlan.h',
        'PrimaryKeyReader.h'
    ],
    deps=['//aios/autil:mem_pool_base', '//aios/storage/indexlib/base:Status']
)
indexlib_cc_library(
    name='primary_key_reader',
    srcs=['PrimaryKeyLoadPlan.cpp'],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':InMemPrimaryKeySegmentReaderTyped', ':PrimaryKeyAttributeReader',
        ':PrimaryKeyBuildingIndexReader', ':PrimaryKeyDuplicationChecker',
        ':PrimaryKeyInDocPositionState', ':PrimaryKeyIndexReader',
        ':PrimaryKeyPostingIterator', ':SegmentDataAdapter',
        ':primary_key_indexer', ':primary_key_reader_interface',
        '//aios/autil:defer', '//aios/storage/indexlib/framework:SegmentMeta',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/index:DocMapDumpParams',
        '//aios/storage/indexlib/index:IIndexReader',
        '//aios/storage/indexlib/index/deletionmap:reader',
        '//aios/storage/indexlib/index/primary_key/merger:OnDiskOrderedPrimaryKeyIterator'
    ]
)
indexlib_cc_library(
    name='PrimaryKeyAttributeReader',
    deps=[
        ':primary_key_indexer',
        '//aios/storage/indexlib/index/attribute:reader',
        '//aios/storage/indexlib/index/primary_key/config:PrimaryKeyIndexConfig'
    ]
)
indexlib_cc_library(name='Common', srcs=[])
indexlib_cc_library(
    name='PrimaryKeyHashConvertor',
    srcs=[],
    deps=['//aios/autil:long_hash_value']
)
indexlib_cc_library(
    name='PrimaryKeyDuplicationChecker',
    deps=[':PrimaryKeyIndexReader', '//aios/autil:thread']
)
indexlib_cc_library(
    name='Types',
    srcs=[],
    deps=['//aios/storage/indexlib/index/common:PrimaryKeyHashType']
)
indexlib_cc_library(name='Constant', srcs=[], deps=[':Types'])
indexlib_cc_library(
    name='SinglePrimaryKeyBuilder', srcs=[], deps=[':primary_key_indexer']
)
indexlib_cc_library(
    name='PrimaryKeyBuildWorkItem',
    srcs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':SinglePrimaryKeyBuilder',
        '//aios/storage/indexlib/index/common:BuildWorkItem'
    ]
)
