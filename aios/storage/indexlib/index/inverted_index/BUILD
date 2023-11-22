load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='factory',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':InvertedIndexFactory']
)
strict_cc_library(
    name='merger',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':InvertedIndexMerger',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/number:NumberIndexMerger'
    ]
)
strict_cc_library(
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
strict_cc_library(
    name='config',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=['//aios/storage/indexlib/index/inverted_index/config']
)
strict_cc_library(
    name='patch',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/index/inverted_index/patch:InvertedIndexPatchFileFinder',
        '//aios/storage/indexlib/index/inverted_index/patch:InvertedIndexPatchIteratorCreator'
    ]
)
strict_cc_library(
    name='constants',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':Common', ':Constant', ':MatchValue']
)
strict_cc_library(name='Common', srcs=[])
strict_cc_library(
    name='IndexQueryCondition', deps=['//aios/autil:json', '//aios/autil:log']
)
strict_cc_library(name='IInvertedDiskIndexer', srcs=[])
strict_cc_library(
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
        '//aios/storage/indexlib/index/inverted_index/patch:IndexUpdateTermIterator',
        '//aios/storage/indexlib/index/inverted_index/section_attribute:factory'
    ]
)
strict_cc_library(
    name='MultiShardInvertedDiskIndexer',
    deps=[
        ':InvertedDiskIndexer', '//aios/storage/indexlib/index:IIndexFactory',
        '//aios/storage/indexlib/index/inverted_index/format:ShardingIndexHasher'
    ]
)
strict_cc_library(
    name='InvertedLeafReader',
    deps=[
        ':Common', ':IndexSegmentReader', ':InvertedIndexSearchTracer',
        ':SegmentPosting', '//aios/autil:memory',
        '//aios/storage/indexlib/index:DiskIndexerParameter',
        '//aios/storage/indexlib/index/inverted_index/format:IndexFormatOption',
        '//aios/storage/indexlib/index/inverted_index/format:TermMetaDumper',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:DictionaryCreator',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:InMemDictionaryReader'
    ]
)
strict_cc_library(
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
strict_cc_library(
    name='MultiFieldIndexReader',
    deps=[
        ':Common', ':IndexAccessoryReader', ':InvertedIndexReader',
        ':KeyIterator',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:DictionaryCreator',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:InMemDictionaryReader'
    ]
)
strict_cc_library(
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
strict_cc_library(
    name='MultiShardInvertedIndexReader',
    deps=[
        ':InvertedIndexReaderImpl', ':MultiShardInvertedDiskIndexer',
        ':MultiShardInvertedMemIndexer',
        '//aios/storage/indexlib/index:IIndexFactory'
    ]
)
strict_cc_library(
    name='InvertedIndexSearchTracer',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/file_system/file:interface',
        '//aios/storage/indexlib/util/cache'
    ]
)
strict_cc_library(
    name='InvertedIndexUtil',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/index/inverted_index/config:InvertedIndexConfig'
    ]
)
strict_cc_library(name='IInvertedMemIndexer', srcs=[])
strict_cc_library(
    name='InvertedMemIndexer',
    deps=[
        ':Common', ':IInvertedMemIndexer', ':IndexFormatWriterCreator',
        ':InvertedIndexFields', ':InvertedIndexMerger', ':InvertedIndexMetrics',
        ':InvertedLeafMemReader', '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/document/extractor:IDocumentInfoExtractor',
        '//aios/storage/indexlib/document/extractor/plain:DocumentInfoExtractorFactory',
        '//aios/storage/indexlib/index/common:FileCompressParamHelper',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:BitmapIndexWriter',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/dynamic:DynamicMemIndexer',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/dynamic:DynamicPostingIterator',
        '//aios/storage/indexlib/index/inverted_index/format/skiplist:BufferedSkipListWriter',
        '//aios/storage/indexlib/index/inverted_index/merge:SimpleInvertedIndexMerger',
        '//aios/storage/indexlib/index/inverted_index/patch:InvertedIndexPatchWriter',
        '//aios/storage/indexlib/index/inverted_index/section_attribute:SectionAttributeMemIndexer'
    ]
)
strict_cc_library(
    name='InvertedIndexMerger',
    deps=[
        ':Common', ':IndexTermExtender', ':MultiSegmentPostingWriter',
        ':PostingMergerImpl', ':SegmentTermInfoQueue',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:SegmentMeta',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResourceManager',
        '//aios/storage/indexlib/index:DocMapper',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/index/common:PlainDocMapper',
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
strict_cc_library(
    name='PostingMerger',
    srcs=[],
    deps=[
        ':IndexOutputSegmentResource',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResource'
    ]
)
strict_cc_library(
    name='PostingMergerImpl',
    deps=[
        ':MultiSegmentPostingWriter', ':OneDocMerger', ':PostingMerger',
        '//aios/storage/indexlib/index:DocMapper'
    ]
)
strict_cc_library(
    name='IndexTermExtender',
    deps=[
        ':PostingMergerImpl',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/adaptive_bitmap:MultiAdaptiveBitmapIndexWriter',
        '//aios/storage/indexlib/index/inverted_index/truncate:TruncateIndexWriter',
        '//aios/storage/indexlib/index/inverted_index/truncate:TruncatePostingIterator',
        '//aios/storage/indexlib/index/inverted_index/truncate:TruncatePostingIteratorCreator'
    ]
)
strict_cc_library(
    name='MultiSegmentPostingWriter',
    deps=[
        ':BufferedPostingIterator', ':IndexOutputSegmentResource',
        ':MultiSegmentPostingIterator', ':PostingWriterImpl',
        '//aios/autil:log', '//aios/storage/indexlib/framework:SegmentMeta',
        '//aios/storage/indexlib/index/inverted_index/format:TermMetaDumper'
    ]
)
strict_cc_library(
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
strict_cc_library(
    name='OnDiskIndexIteratorCreator',
    srcs=[],
    deps=[
        ':Common', ':OnDiskIndexIterator',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:OnDiskBitmapIndexIterator',
        '//aios/storage/indexlib/index/inverted_index/format:PostingFormatOption',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:DictionaryReader'
    ]
)
strict_cc_library(
    name='InvertedIndexMetrics',
    deps=[
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/framework:IMetrics',
        '//aios/storage/indexlib/framework:MetricsWrapper',
        '//aios/storage/indexlib/index:IndexReaderParameter'
    ]
)
strict_cc_library(
    name='InvertedIndexFactory',
    deps=[
        ':InvertedDiskIndexer', ':InvertedIndexFieldsParser',
        ':InvertedIndexMetrics', ':InvertedIndexReaderImpl',
        ':InvertedMemIndexer', ':MultiShardInvertedDiskIndexer',
        ':MultiShardInvertedIndexReader', ':MultiShardInvertedMemIndexer',
        '//aios/autil:log', '//aios/storage/indexlib/index:IIndexFactory',
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
strict_cc_library(
    name='MultiShardInvertedMemIndexer',
    deps=[
        ':InvertedMemIndexer', '//aios/autil:log',
        '//aios/storage/indexlib/document:DocumentBatch',
        '//aios/storage/indexlib/index:IIndexFactory',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/inverted_index/format:ShardingIndexHasher',
        '//aios/storage/indexlib/index/inverted_index/patch:IndexUpdateTermIterator',
        '//aios/storage/indexlib/index/inverted_index/section_attribute:SectionAttributeMemIndexer'
    ]
)
strict_cc_library(
    name='IndexAccessoryReader',
    deps=[
        ':Common', '//aios/autil:log', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/index:DiskIndexerParameter',
        '//aios/storage/indexlib/index/inverted_index/section_attribute:SectionAttributeReaderImpl'
    ]
)
strict_cc_library(
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
strict_cc_library(name='SegmentPostings', srcs=[], deps=[':SegmentPosting'])
strict_cc_library(
    name='PostingWriter',
    srcs=[],
    deps=[
        '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/index/inverted_index/format:PostingFormat',
        '//aios/storage/indexlib/util:simple_pool'
    ]
)
strict_cc_library(
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
strict_cc_library(
    name='PostingExecutor',
    srcs=[],
    deps=[':Constant', '//aios/storage/indexlib/base:constants']
)
strict_cc_library(
    name='AndPostingExecutor', deps=[':PostingExecutor', '//aios/autil:log']
)
strict_cc_library(
    name='DocidRangePostingExecutor', srcs=[], deps=[':PostingExecutor']
)
strict_cc_library(
    name='OrPostingExecutor', deps=[':PostingExecutor', '//aios/autil:log']
)
strict_cc_library(
    name='TermPostingExecutor',
    deps=[
        ':PostingExecutor', ':PostingIterator',
        '//aios/storage/indexlib/index/inverted_index/format:TermMeta',
        '//aios/storage/indexlib/index/inverted_index/format:TermMetaDumper',
        '//aios/storage/indexlib/index/inverted_index/format:TermMetaLoader'
    ]
)
strict_cc_library(name='PostingDumper', srcs=[], deps=[':PostingWriter'])
strict_cc_library(name='DocValueFilter', srcs=[])
strict_cc_library(
    name='NumberDocValueFilterTyped',
    srcs=[],
    deps=['DocValueFilter', '//aios/storage/indexlib/index/attribute:reader']
)
strict_cc_library(
    name='RangePostingIterator',
    deps=[
        ':PostingIteratorImpl', ':RangeSegmentPostingsIterator',
        ':SegmentPostings', ':TermMatchData'
    ]
)
strict_cc_library(
    name='DefaultValueIndexIterator',
    deps=[
        ':IndexIterator',
        '//aios/storage/indexlib/index/inverted_index/format:PostingDecoderImpl',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:DefaultTermDictionaryReader'
    ]
)
strict_cc_library(
    name='MultiSegmentPostingIterator',
    deps=[
        ':PostingIteratorImpl',
        '//aios/storage/indexlib/index/inverted_index/format:TermMetaDumper'
    ]
)
strict_cc_library(
    name='BufferedPostingIterator',
    deps=[
        ':BufferedIndexDecoder', ':PostingIteratorImpl', ':SegmentPosting',
        ':TermMatchData',
        '//aios/storage/indexlib/index/common/numeric_compress:ReferenceCompressIntEncoder',
        '//aios/storage/indexlib/index/inverted_index/format:NormalInDoc'
    ]
)
strict_cc_library(
    name='PositionIteratorTyped',
    srcs=[],
    deps=[
        ':PostingIteratorImpl',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:SingleBitmapPostingIterator'
    ]
)
strict_cc_library(
    name='CompositePostingIterator',
    srcs=[],
    deps=[
        ':BufferedPostingIterator',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/dynamic:DynamicPostingIterator'
    ]
)
strict_cc_library(
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
strict_cc_library(
    name='InDocStateKeeper',
    deps=[
        '//aios/storage/indexlib/index/inverted_index/format:InMemPositionListDecoder',
        '//aios/storage/indexlib/index/inverted_index/format:TermMeta',
        '//aios/storage/indexlib/index/inverted_index/format/skiplist:PairValueSkipListReader'
    ]
)
strict_cc_library(
    name='PostingIteratorImpl',
    deps=[
        ':InvertedIndexSearchTracer', ':MultiSegmentTermMetaCalculator',
        ':PostingIterator', ':SegmentPosting',
        '//aios/storage/indexlib/file_system:byte_slice_rw',
        '//aios/storage/indexlib/util:PooledUniquePtr',
        '//aios/storage/indexlib/util:object_pool'
    ]
)
strict_cc_library(
    name='SeekAndFilterIterator',
    deps=[':DocValueFilter', ':RangePostingIterator']
)
strict_cc_library(
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
strict_cc_library(
    name='MultiSegmentTermMetaCalculator',
    srcs=[],
    deps=['//aios/storage/indexlib/index/inverted_index/format:TermMeta']
)
strict_cc_library(
    name='IndexIterator',
    srcs=[],
    deps=['//aios/storage/indexlib/index/common:DictKeyInfo']
)
strict_cc_library(name='KeyIterator', srcs=[])
strict_cc_library(name='KeyIteratorTyped', srcs=[], deps=[':KeyIterator'])
strict_cc_library(
    name='OnDiskIndexIterator',
    srcs=[],
    deps=[
        ':IndexIterator',
        '//aios/storage/indexlib/index/inverted_index/format:PostingFormatOption'
    ]
)
strict_cc_library(
    name='BuildingIndexReader',
    deps=[
        ':IndexSegmentReader', ':InvertedIndexSearchTracer', ':SegmentPosting',
        '//aios/storage/indexlib/document/normal:ModifiedTokens'
    ]
)
strict_cc_library(
    name='IndexDataWriter',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:TieredDictionaryWriter'
    ]
)
strict_cc_library(
    name='InvertedIndexReader',
    deps=[
        ':PostingIterator', '//aios/storage/indexlib/index:IIndexReader',
        '//aios/storage/indexlib/document:IIndexFields',
        '//aios/storage/indexlib/document/normal:ModifiedTokens',
        '//aios/storage/indexlib/framework:SegmentInfo',
        '//aios/storage/indexlib/index/inverted_index/format:DocListFormatOption',
        '//aios/storage/indexlib/index/inverted_index/format:IndexFormatOption',
        '//aios/storage/indexlib/index/inverted_index/format:PositionListFormatOption',
        '//aios/storage/indexlib/index/inverted_index/format:PostingFormatOption'
    ]
)
strict_cc_library(
    name='PostingIterator',
    srcs=[],
    deps=[
        ':TermPostingInfo', '//aios/storage/indexlib/index/common:error_code',
        '//aios/storage/indexlib/index/inverted_index/format:TermMeta'
    ]
)
strict_cc_library(
    name='InDocPositionIterator',
    srcs=[],
    deps=['//aios/storage/indexlib/index/common:error_code']
)
strict_cc_library(
    name='InDocPositionState',
    srcs=[],
    deps=[
        ':InDocPositionIterator', ':SectionAttributeReader',
        '//aios/storage/indexlib/base:Constant',
        '//aios/storage/indexlib/index/inverted_index:Types',
        '//aios/storage/indexlib/index/inverted_index/format:PositionListFormatOption'
    ]
)
strict_cc_library(
    name='OneDocMerger',
    deps=[
        ':PostingWriterImpl', '//aios/autil:string_type',
        '//aios/storage/indexlib/index/common/numeric_compress:ReferenceCompressIntEncoder',
        '//aios/storage/indexlib/index/inverted_index/format:PostingDecoderImpl',
        '//aios/storage/indexlib/index/inverted_index/format:PostingFormatOption',
        '//aios/storage/indexlib/index/inverted_index/patch:SingleTermIndexSegmentPatchIterator'
    ]
)
strict_cc_library(
    name='TermPostingInfo',
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/file_system:byte_slice_rw'
    ]
)
strict_cc_library(
    name='SegmentTermInfo',
    deps=[
        ':IndexIterator',
        '//aios/storage/indexlib/index/inverted_index/patch:SingleFieldIndexSegmentPatchIterator',
        '//aios/storage/indexlib/index/inverted_index/patch:SingleTermIndexSegmentPatchIterator'
    ]
)
strict_cc_library(
    name='IndexSegmentReader',
    srcs=[],
    deps=[
        ':SegmentPosting',
        '//aios/storage/indexlib/document/normal:ModifiedTokens'
    ]
)
strict_cc_library(
    name='TermMatchData',
    srcs=[],
    deps=[':InDocPositionIterator', ':InDocPositionState']
)
strict_cc_library(name='SectionAttributeReader', srcs=[])
strict_cc_library(
    name='InDocSectionMeta',
    srcs=[],
    deps=[':Types', '//aios/storage/indexlib/base:Types']
)
strict_cc_library(
    name='IndexFormatWriterCreator',
    deps=[
        ':PostingWriter', ':PostingWriterImpl',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:BitmapIndexWriter',
        '//aios/storage/indexlib/index/inverted_index/format:IndexFormatOption',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:DictionaryCreator'
    ]
)
strict_cc_library(
    name='IndexOutputSegmentResource',
    deps=[
        ':IndexDataWriter', ':SegmentTermInfo',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/inverted_index/format/dictionary:DictionaryCreator'
    ]
)
strict_cc_library(
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
strict_cc_library(
    name='SingleInvertedIndexBuilder',
    deps=[
        ':InvertedIndexerOrganizerUtil', ':MultiShardInvertedDiskIndexer',
        ':MultiShardInvertedMemIndexer'
    ]
)
strict_cc_library(
    name='InvertedIndexBuildWorkItem',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':SingleInvertedIndexBuilder',
        '//aios/storage/indexlib/index/common:BuildWorkItem'
    ]
)
strict_cc_library(
    name='InvertedIndexModifier',
    srcs=[],
    hdrs=['InvertedIndexModifier.h'],
    deps=[
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/document/normal:NormalDocument'
    ]
)
strict_cc_library(
    name='InplaceInvertedIndexModifier',
    deps=[
        ':InvertedIndexModifier', ':InvertedIndexerOrganizerUtil',
        '//aios/storage/indexlib/document:interface',
        '//aios/storage/indexlib/index/common:IndexerOrganizerUtil'
    ]
)
strict_cc_library(
    name='PatchInvertedIndexModifier',
    deps=[
        ':InvertedIndexModifier', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/index/inverted_index/patch:InvertedIndexPatchWriter'
    ]
)
strict_cc_library(
    name='Constant',
    srcs=[],
    deps=[
        ':Types', '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/index/common:Types'
    ]
)
strict_cc_library(name='Types', srcs=[])
strict_cc_library(name='MatchValue', srcs=[])
strict_cc_library(
    name='InvertedIndexFields',
    deps=[
        ':Common', ':Types', '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/document:IIndexFields',
        '//aios/storage/indexlib/document/normal:Field',
        '//aios/storage/indexlib/document/normal:IndexRawField',
        '//aios/storage/indexlib/document/normal:IndexTokenizeField',
        '//aios/storage/indexlib/document/normal:NullField',
        '//aios/storage/indexlib/document/normal/tokenize'
    ]
)
strict_cc_library(
    name='InvertedIndexFieldsParser',
    deps=[
        ':InvertedIndexFields', ':config',
        '//aios/storage/indexlib/config:FieldConfig',
        '//aios/storage/indexlib/document:DocumentInitParam',
        '//aios/storage/indexlib/document:ExtendDocument',
        '//aios/storage/indexlib/document:IIndexFieldsParser',
        '//aios/storage/indexlib/document:RawDocument',
        '//aios/storage/indexlib/document/normal:TokenizeDocumentConvertor',
        '//aios/storage/indexlib/index/common/field_format/date:DateFieldEncoder',
        '//aios/storage/indexlib/index/common/field_format/range:RangeFieldEncoder',
        '//aios/storage/indexlib/index/common/field_format/spatial:shape',
        '//aios/storage/indexlib/util:PooledUniquePtr'
    ]
)
