load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
indexlib_cc_library(
    name='factory',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':InvertedIndexFactory']
)
indexlib_cc_library(
    name='merger',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':InvertedIndexMerger',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/number:NumberIndexMerger'
    ]
)
indexlib_cc_library(
    name='executor',
    srcs=[],
    hdrs=[],
    visibility=[
        '//aios/apps/facility/build_service:__subpackages__',
        '//aios/storage/indexlib:__subpackages__'
    ],
    deps=[
        ':AndPostingExecutor', ':DocidRangePostingExecutor',
        ':OrPostingExecutor', ':TermPostingExecutor'
    ]
)
indexlib_cc_library(
    name='config',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=['//aios/storage/indexlib/index/inverted_index/config']
)
indexlib_cc_library(
    name='patch',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/index/inverted_index/patch:InvertedIndexPatchFileFinder',
        '//aios/storage/indexlib/index/inverted_index/patch:InvertedIndexPatchIteratorCreator'
    ]
)
indexlib_cc_library(
    name='constants',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':Common', ':Constant', ':MatchValue']
)
indexlib_cc_library(name='Common', srcs=[])
indexlib_cc_library(
    name='IndexQueryCondition', deps=['//aios/autil:json', '//aios/autil:log']
)
indexlib_cc_library(name='IInvertedDiskIndexer', srcs=[])
indexlib_cc_library(
    name='InvertedDiskIndexer',
    hdrs=['InvertedDiskIndexer.h'],
    deps=[
        ':Common', ':IInvertedDiskIndexer', ':InvertedLeafReader',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/index:IndexFactoryCreator',
        '//aios/storage/indexlib/index/attribute:indexer',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:BitmapDiskIndexer',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/dynamic:DynamicMemIndexer',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:DefaultTermDictionaryReader',
        '//aios/storage/indexlib/index/inverted_index/patch:IndexUpdateTermIterator'
    ]
)
indexlib_cc_library(
    name='MultiShardInvertedDiskIndexer',
    deps=[
        ':InvertedDiskIndexer', '//aios/storage/indexlib/index:IIndexFactory',
        '//aios/storage/indexlib/index/inverted_index/format:ShardingIndexHasher'
    ]
)
indexlib_cc_library(
    name='InvertedLeafReader',
    deps=[
        ':Common', ':IndexSegmentReader', ':InvertedIndexSearchTracer',
        ':SegmentPosting', '//aios/autil:memory',
        '//aios/storage/indexlib/index:IndexerParameter',
        '//aios/storage/indexlib/index/inverted_index/format:IndexFormatOption',
        '//aios/storage/indexlib/index/inverted_index/format:TermMetaDumper',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:DictionaryCreator',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:InMemDictionaryReader'
    ]
)
indexlib_cc_library(
    name='InvertedLeafMemReader',
    deps=[
        ':Common', ':IndexSegmentReader', ':InvertedIndexSearchTracer',
        ':PostingWriter', ':SegmentPosting', '//aios/autil:snapshot_vector',
        '//aios/storage/indexlib/index/attribute:reader',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:InMemBitmapIndexSegmentReader',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/dynamic:DynamicIndexSegmentReader',
        '//aios/storage/indexlib/index/inverted_index/format:IndexFormatOption',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:InMemDictionaryReader'
    ]
)
indexlib_cc_library(
    name='MultiFieldIndexReader',
    deps=[
        ':Common', ':IndexAccessoryReader', ':InvertedIndexReader',
        ':KeyIterator',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:DictionaryCreator',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:InMemDictionaryReader'
    ]
)
indexlib_cc_library(
    name='InvertedIndexReaderImpl',
    deps=[
        ':BufferedPostingIterator', ':BuildingIndexReader', ':Common',
        ':CompositePostingIterator', ':IndexAccessoryReader',
        ':InvertedDiskIndexer', ':InvertedIndexMetrics',
        ':InvertedIndexSearchTracer', ':InvertedLeafReader',
        ':InvertedMemIndexer', ':KeyIteratorTyped', ':MultiFieldIndexReader',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:BitmapDiskIndexer',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:BitmapIndexReader',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/dynamic:DynamicIndexReader'
    ]
)
indexlib_cc_library(
    name='MultiShardInvertedIndexReader',
    deps=[
        ':InvertedIndexReaderImpl', ':MultiShardInvertedDiskIndexer',
        ':MultiShardInvertedMemIndexer',
        '//aios/storage/indexlib/index:IIndexFactory'
    ]
)
indexlib_cc_library(
    name='InvertedIndexSearchTracer',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/file_system/file:interface',
        '//aios/storage/indexlib/util/cache'
    ]
)
indexlib_cc_library(
    name='InvertedIndexUtil',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/index/inverted_index/config:InvertedIndexConfig'
    ]
)
indexlib_cc_library(name='IInvertedMemIndexer', srcs=[])
indexlib_cc_library(
    name='InvertedMemIndexer',
    deps=[
        ':Common', ':IInvertedMemIndexer', ':IndexFormatWriterCreator',
        ':InvertedIndexMerger', ':InvertedIndexMetrics',
        ':InvertedLeafMemReader', ':SectionAttributeMemIndexer',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/document/extractor/plain:DocumentInfoExtractorFactory',
        '//aios/storage/indexlib/index/common:FileCompressParamHelper',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:BitmapIndexWriter',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/dynamic:DynamicMemIndexer',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/dynamic:DynamicPostingIterator',
        '//aios/storage/indexlib/index/inverted_index/format/skiplist:BufferedSkipListWriter',
        '//aios/storage/indexlib/index/inverted_index/merge:SimpleInvertedIndexMerger',
        '//aios/storage/indexlib/index/inverted_index/patch:InvertedIndexPatchWriter'
    ]
)
indexlib_cc_library(
    name='InvertedIndexMerger',
    deps=[
        ':Common', ':IndexTermExtender', ':MultiSegmentPostingWriter',
        ':PostingMergerImpl', ':SegmentTermInfoQueue',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:SegmentMeta',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResourceManager',
        '//aios/storage/indexlib/index:DocMapper',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/adaptive_bitmap:AdaptiveBitmapIndexWriterCreator',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/adaptive_bitmap:MultiAdaptiveBitmapIndexWriter',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:BitmapPostingMerger',
        '//aios/storage/indexlib/index/inverted_index/patch:InvertedIndexDedupPatchFileMerger',
        '//aios/storage/indexlib/index/inverted_index/truncate:BucketMap',
        '//aios/storage/indexlib/index/inverted_index/truncate:BucketMapCreator',
        '//aios/storage/indexlib/index/inverted_index/truncate:TruncateAttributeReaderCreator',
        '//aios/storage/indexlib/index/inverted_index/truncate:TruncateIndexWriterCreator'
    ]
)
indexlib_cc_library(
    name='PostingMerger',
    srcs=[],
    deps=[
        ':IndexOutputSegmentResource',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResource'
    ]
)
indexlib_cc_library(
    name='PostingMergerImpl',
    deps=[
        ':MultiSegmentPostingWriter', ':OneDocMerger', ':PostingMerger',
        '//aios/storage/indexlib/index:DocMapper'
    ]
)
indexlib_cc_library(
    name='IndexTermExtender',
    deps=[
        ':PostingMergerImpl',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/adaptive_bitmap:MultiAdaptiveBitmapIndexWriter',
        '//aios/storage/indexlib/index/inverted_index/truncate:TruncateIndexWriter',
        '//aios/storage/indexlib/index/inverted_index/truncate:TruncatePostingIterator',
        '//aios/storage/indexlib/index/inverted_index/truncate:TruncatePostingIteratorCreator'
    ]
)
indexlib_cc_library(
    name='MultiSegmentPostingWriter',
    deps=[
        ':BufferedPostingIterator', ':IndexOutputSegmentResource',
        ':MultiSegmentPostingIterator', ':PostingWriterImpl',
        '//aios/autil:log', '//aios/storage/indexlib/framework:SegmentMeta',
        '//aios/storage/indexlib/index/inverted_index/format:TermMetaDumper'
    ]
)
indexlib_cc_library(
    name='SegmentTermInfoQueue',
    deps=[
        ':DefaultValueIndexIterator', ':IndexIterator',
        ':OnDiskIndexIteratorCreator', ':SegmentTermInfo',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/index/common/patch:PatchFileInfos',
        '//aios/storage/indexlib/index/inverted_index/patch:InvertedIndexPatchFileFinder'
    ]
)
indexlib_cc_library(
    name='OnDiskIndexIteratorCreator',
    srcs=[],
    deps=[
        ':Common', ':OnDiskIndexIterator',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:OnDiskBitmapIndexIterator',
        '//aios/storage/indexlib/index/inverted_index/format:PostingFormatOption',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:DictionaryReader'
    ]
)
indexlib_cc_library(
    name='InvertedIndexMetrics',
    deps=[
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/framework:IMetrics',
        '//aios/storage/indexlib/framework:MetricsWrapper',
        '//aios/storage/indexlib/index:IndexerParameter'
    ]
)
indexlib_cc_library(
    name='InvertedIndexFactory',
    deps=[
        ':InvertedDiskIndexer', ':InvertedIndexMetrics',
        ':InvertedIndexReaderImpl', ':InvertedMemIndexer',
        ':MultiShardInvertedDiskIndexer', ':MultiShardInvertedIndexReader',
        ':MultiShardInvertedMemIndexer', '//aios/autil:log',
        '//aios/storage/indexlib/index:IIndexFactory',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/index:IIndexReader',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/date:DateDiskIndexer',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/date:DateIndexMerger',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/date:DateIndexReader',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/date:DateMemIndexer',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/expack:ExpackIndexMerger',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/number:NumberIndexMerger',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/pack:PackIndexMerger',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/range:RangeDiskIndexer',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/range:RangeIndexMerger',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/range:RangeIndexReader',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/range:RangeMemIndexer',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/spatial:SpatialIndexMerger',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/spatial:SpatialIndexReader',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/string:StringIndexMerger',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/text:TextIndexMerger'
    ],
    alwayslink=True
)
indexlib_cc_library(
    name='MultiShardInvertedMemIndexer',
    deps=[
        ':InvertedMemIndexer', ':SectionAttributeMemIndexer',
        '//aios/autil:log', '//aios/storage/indexlib/index:IIndexFactory',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/inverted_index/format:ShardingIndexHasher',
        '//aios/storage/indexlib/index/inverted_index/patch:IndexUpdateTermIterator'
    ]
)
indexlib_cc_library(
    name='SectionAttributeMemIndexer',
    deps=[
        '//aios/autil:log',
        '//aios/storage/indexlib/document/normal:IndexDocument',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/attribute:indexer',
        '//aios/storage/indexlib/index/inverted_index/config:PackageIndexConfig'
    ]
)
indexlib_cc_library(
    name='IndexAccessoryReader',
    deps=[
        ':Common', '//aios/autil:log', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/index:IndexerParameter',
        '//aios/storage/indexlib/index/inverted_index/section_attribute:SectionAttributeReaderImpl'
    ]
)
indexlib_cc_library(
    name='SegmentPosting',
    deps=[
        ':PostingWriter', '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/file_system:byte_slice_rw',
        '//aios/storage/indexlib/framework:SegmentInfo',
        '//aios/storage/indexlib/index/inverted_index/format:DictInlineDecoder',
        '//aios/storage/indexlib/index/inverted_index/format:DictInlineFormatter',
        '//aios/storage/indexlib/index/inverted_index/format:PostingFormatOption',
        '//aios/storage/indexlib/index/inverted_index/format:ShortListOptimizeUtil',
        '//aios/storage/indexlib/index/inverted_index/format:TermMeta',
        '//aios/storage/indexlib/index/inverted_index/format:TermMetaLoader'
    ]
)
indexlib_cc_library(name='SegmentPostings', srcs=[], deps=[':SegmentPosting'])
indexlib_cc_library(
    name='PostingWriter',
    srcs=[],
    deps=[
        '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/index/inverted_index/format:PostingFormat',
        '//aios/storage/indexlib/util:simple_pool'
    ]
)
indexlib_cc_library(
    name='PostingWriterImpl',
    deps=[
        ':BufferedPostingIterator', ':PostingIterator', ':PostingWriter',
        ':SegmentPostings', ':TermMatchData',
        '//aios/storage/indexlib/index/inverted_index/format:DocListEncoder',
        '//aios/storage/indexlib/index/inverted_index/format:InMemDictInlineDocListDecoder',
        '//aios/storage/indexlib/index/inverted_index/format:PositionListEncoder',
        '//aios/storage/indexlib/util:object_pool'
    ]
)
indexlib_cc_library(
    name='PostingExecutor',
    srcs=[],
    deps=[':Constant', '//aios/storage/indexlib/base:constants']
)
indexlib_cc_library(
    name='AndPostingExecutor', deps=[':PostingExecutor', '//aios/autil:log']
)
indexlib_cc_library(
    name='DocidRangePostingExecutor', srcs=[], deps=[':PostingExecutor']
)
indexlib_cc_library(
    name='OrPostingExecutor', deps=[':PostingExecutor', '//aios/autil:log']
)
indexlib_cc_library(
    name='TermPostingExecutor',
    deps=[
        ':PostingExecutor', ':PostingIterator',
        '//aios/storage/indexlib/index/inverted_index/format:TermMeta',
        '//aios/storage/indexlib/index/inverted_index/format:TermMetaDumper',
        '//aios/storage/indexlib/index/inverted_index/format:TermMetaLoader'
    ]
)
indexlib_cc_library(name='PostingDumper', srcs=[], deps=[':PostingWriter'])
indexlib_cc_library(name='DocValueFilter', srcs=[])
indexlib_cc_library(
    name='NumberDocValueFilterTyped',
    srcs=[],
    deps=['DocValueFilter', '//aios/storage/indexlib/index/attribute:reader']
)
indexlib_cc_library(
    name='RangePostingIterator',
    deps=[
        ':PostingIteratorImpl', ':RangeSegmentPostingsIterator',
        ':SegmentPostings', ':TermMatchData'
    ]
)
indexlib_cc_library(
    name='DefaultValueIndexIterator',
    deps=[
        ':IndexIterator',
        '//aios/storage/indexlib/index/inverted_index/format:PostingDecoderImpl',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:DefaultTermDictionaryReader'
    ]
)
indexlib_cc_library(
    name='MultiSegmentPostingIterator',
    deps=[
        ':PostingIteratorImpl',
        '//aios/storage/indexlib/index/inverted_index/format:TermMetaDumper'
    ]
)
indexlib_cc_library(
    name='BufferedPostingIterator',
    deps=[
        ':BufferedIndexDecoder', ':PostingIteratorImpl', ':SegmentPosting',
        ':TermMatchData',
        '//aios/storage/indexlib/index/common/numeric_compress:ReferenceCompressIntEncoder',
        '//aios/storage/indexlib/index/inverted_index/format:NormalInDoc'
    ]
)
indexlib_cc_library(
    name='PositionIteratorTyped',
    srcs=[],
    deps=[
        ':PostingIteratorImpl',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:SingleBitmapPostingIterator'
    ]
)
indexlib_cc_library(
    name='CompositePostingIterator',
    srcs=[],
    deps=[
        ':BufferedPostingIterator',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/dynamic:DynamicPostingIterator'
    ]
)
indexlib_cc_library(
    name='BufferedIndexDecoder',
    deps=[
        ':InDocStateKeeper', ':SegmentPosting',
        '//aios/storage/indexlib/index/inverted_index/format:BufferedSegmentIndexDecoder',
        '//aios/storage/indexlib/index/inverted_index/format:DictInlinePostingDecoder',
        '//aios/storage/indexlib/index/inverted_index/format:InMemPostingDecoder',
        '//aios/storage/indexlib/index/inverted_index/format:ShortListSegmentDecoder',
        '//aios/storage/indexlib/index/inverted_index/format:SkipListSegmentDecoder',
        '//aios/storage/indexlib/index/inverted_index/format/skiplist:TriValueSkipListReader'
    ]
)
indexlib_cc_library(
    name='InDocStateKeeper',
    deps=[
        '//aios/storage/indexlib/index/inverted_index/format:InMemPositionListDecoder',
        '//aios/storage/indexlib/index/inverted_index/format:TermMeta',
        '//aios/storage/indexlib/index/inverted_index/format/skiplist:PairValueSkipListReader'
    ]
)
indexlib_cc_library(
    name='PostingIteratorImpl',
    deps=[
        ':InvertedIndexSearchTracer', ':MultiSegmentTermMetaCalculator',
        ':PostingIterator', ':SegmentPosting',
        '//aios/storage/indexlib/file_system:byte_slice_rw',
        '//aios/storage/indexlib/util:PooledUniquePtr',
        '//aios/storage/indexlib/util:object_pool'
    ]
)
indexlib_cc_library(
    name='SeekAndFilterIterator',
    deps=[':DocValueFilter', ':RangePostingIterator']
)
indexlib_cc_library(
    name='RangeSegmentPostingsIterator',
    deps=[
        ':SegmentPosting', ':SegmentPostings',
        '//aios/storage/indexlib/index/inverted_index/format:BufferedSegmentIndexDecoder',
        '//aios/storage/indexlib/index/inverted_index/format:DictInlinePostingDecoder',
        '//aios/storage/indexlib/index/inverted_index/format:InMemPostingDecoder',
        '//aios/storage/indexlib/index/inverted_index/format:ShortListSegmentDecoder',
        '//aios/storage/indexlib/index/inverted_index/format:SkipListSegmentDecoder',
        '//aios/storage/indexlib/index/inverted_index/format:TermMetaLoader',
        '//aios/storage/indexlib/index/inverted_index/format/skiplist:PairValueSkipListReader',
        '//aios/storage/indexlib/index/inverted_index/format/skiplist:TriValueSkipListReader',
        '//aios/storage/indexlib/util:simple_heap'
    ]
)
indexlib_cc_library(
    name='MultiSegmentTermMetaCalculator',
    srcs=[],
    deps=['//aios/storage/indexlib/index/inverted_index/format:TermMeta']
)
indexlib_cc_library(
    name='IndexIterator',
    srcs=[],
    deps=['//aios/storage/indexlib/index/common:DictKeyInfo']
)
indexlib_cc_library(name='KeyIterator', srcs=[])
indexlib_cc_library(name='KeyIteratorTyped', srcs=[], deps=[':KeyIterator'])
indexlib_cc_library(
    name='OnDiskIndexIterator',
    srcs=[],
    deps=[
        ':IndexIterator',
        '//aios/storage/indexlib/index/inverted_index/format:PostingFormatOption'
    ]
)
indexlib_cc_library(
    name='BuildingIndexReader',
    deps=[
        ':IndexSegmentReader', ':InvertedIndexSearchTracer', ':SegmentPosting',
        '//aios/storage/indexlib/document/normal:ModifiedTokens'
    ]
)
indexlib_cc_library(
    name='IndexDataWriter',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:TieredDictionaryWriter'
    ]
)
indexlib_cc_library(
    name='InvertedIndexReader',
    deps=[
        ':PostingIterator',
        '//aios/storage/indexlib/document/normal:ModifiedTokens',
        '//aios/storage/indexlib/framework:SegmentInfo',
        '//aios/storage/indexlib/index:IIndexReader',
        '//aios/storage/indexlib/index/inverted_index/format:DocListFormatOption',
        '//aios/storage/indexlib/index/inverted_index/format:IndexFormatOption',
        '//aios/storage/indexlib/index/inverted_index/format:PositionListFormatOption',
        '//aios/storage/indexlib/index/inverted_index/format:PostingFormatOption'
    ]
)
indexlib_cc_library(
    name='PostingIterator',
    srcs=[],
    deps=[
        ':TermPostingInfo', '//aios/storage/indexlib/index/common:error_code',
        '//aios/storage/indexlib/index/inverted_index/format:TermMeta'
    ]
)
indexlib_cc_library(
    name='InDocPositionIterator',
    srcs=[],
    deps=['//aios/storage/indexlib/index/common:error_code']
)
indexlib_cc_library(
    name='InDocPositionState',
    srcs=[],
    deps=[
        ':InDocPositionIterator', ':SectionAttributeReader',
        '//aios/storage/indexlib/base:Constant',
        '//aios/storage/indexlib/index/inverted_index:Types',
        '//aios/storage/indexlib/index/inverted_index/format:PositionListFormatOption'
    ]
)
indexlib_cc_library(
    name='OneDocMerger',
    deps=[
        ':PostingWriterImpl', '//aios/autil:string_type',
        '//aios/storage/indexlib/index/common/numeric_compress:ReferenceCompressIntEncoder',
        '//aios/storage/indexlib/index/inverted_index/format:PostingDecoderImpl',
        '//aios/storage/indexlib/index/inverted_index/format:PostingFormatOption',
        '//aios/storage/indexlib/index/inverted_index/patch:SingleTermIndexSegmentPatchIterator'
    ]
)
indexlib_cc_library(
    name='TermPostingInfo',
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/file_system:byte_slice_rw'
    ]
)
indexlib_cc_library(
    name='SegmentTermInfo',
    deps=[
        ':IndexIterator',
        '//aios/storage/indexlib/index/inverted_index/patch:SingleFieldIndexSegmentPatchIterator',
        '//aios/storage/indexlib/index/inverted_index/patch:SingleTermIndexSegmentPatchIterator'
    ]
)
indexlib_cc_library(
    name='IndexSegmentReader',
    srcs=[],
    deps=[
        ':SegmentPosting',
        '//aios/storage/indexlib/document/normal:ModifiedTokens'
    ]
)
indexlib_cc_library(
    name='TermMatchData',
    srcs=[],
    deps=[':InDocPositionIterator', ':InDocPositionState']
)
indexlib_cc_library(name='SectionAttributeReader', srcs=[])
indexlib_cc_library(
    name='InDocSectionMeta',
    srcs=[],
    deps=[':Types', '//aios/storage/indexlib/base:Types']
)
indexlib_cc_library(
    name='IndexFormatWriterCreator',
    deps=[
        ':PostingWriter', ':PostingWriterImpl',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:BitmapIndexWriter',
        '//aios/storage/indexlib/index/inverted_index/format:IndexFormatOption',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:DictionaryCreator'
    ]
)
indexlib_cc_library(
    name='IndexOutputSegmentResource',
    deps=[
        ':IndexDataWriter', ':SegmentTermInfo',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:DictionaryCreator'
    ]
)
indexlib_cc_library(
    name='InvertedIndexerOrganizerUtil',
    deps=[
        ':InvertedDiskIndexer', ':InvertedIndexMetrics', ':InvertedMemIndexer',
        ':MultiShardInvertedDiskIndexer', ':MultiShardInvertedMemIndexer',
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/index/common:Constant',
        '//aios/storage/indexlib/index/common:IndexerOrganizerMeta',
        '//aios/storage/indexlib/index/common:IndexerOrganizerUtil',
        '//aios/storage/indexlib/index/common:Types'
    ]
)
indexlib_cc_library(
    name='SingleInvertedIndexBuilder',
    deps=[
        ':InvertedIndexerOrganizerUtil', ':MultiShardInvertedDiskIndexer',
        ':MultiShardInvertedMemIndexer'
    ]
)
indexlib_cc_library(
    name='InvertedIndexBuildWorkItem',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':SingleInvertedIndexBuilder',
        '//aios/storage/indexlib/index/common:BuildWorkItem'
    ]
)
indexlib_cc_library(
    name='InvertedIndexModifier',
    srcs=[],
    hdrs=['InvertedIndexModifier.h'],
    deps=[
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/document/normal:NormalDocument'
    ]
)
indexlib_cc_library(
    name='InplaceInvertedIndexModifier',
    deps=[
        ':InvertedIndexModifier', ':InvertedIndexerOrganizerUtil',
        '//aios/storage/indexlib/document:interface',
        '//aios/storage/indexlib/index/common:IndexerOrganizerUtil'
    ]
)
indexlib_cc_library(
    name='PatchInvertedIndexModifier',
    deps=[
        ':InvertedIndexModifier', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/index/inverted_index/patch:InvertedIndexPatchWriter'
    ]
)
indexlib_cc_library(
    name='Constant',
    srcs=[],
    deps=[
        ':Types', '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/index/common:Types'
    ]
)
indexlib_cc_library(name='Types', srcs=[])
indexlib_cc_library(name='MatchValue', srcs=[])
