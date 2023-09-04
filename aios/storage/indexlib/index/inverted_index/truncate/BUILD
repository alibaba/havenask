load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='TruncateAttributeReader',
    deps=[
        '//aios/autil:log', '//aios/autil:memory',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/index:DocMapper',
        '//aios/storage/indexlib/index/attribute:indexer',
        '//aios/storage/indexlib/index/attribute/config:AttributeConfig'
    ]
)
strict_cc_library(
    name='TruncateAttributeReaderCreator',
    deps=[
        ':TruncateAttributeReader', '//aios/autil:log', '//aios/autil:memory',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/index:DocMapper',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/index/attribute:indexer',
        '//aios/storage/indexlib/index/attribute/config'
    ]
)
strict_cc_library(
    name='SortWorkItem',
    deps=[
        ':BucketMap', ':TruncateAttributeReader',
        ':TruncateAttributeReaderCreator', '//aios/autil:log',
        '//aios/autil:memory', '//aios/autil:thread',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/index:DocMapper',
        '//aios/storage/indexlib/index/attribute:constants',
        '//aios/storage/indexlib/index/attribute:indexer',
        '//aios/storage/indexlib/index/inverted_index/config:TruncateOptionConfig',
        '//aios/storage/indexlib/index/inverted_index/config:TruncateProfile'
    ]
)
strict_cc_library(
    name='TruncateMetaReader',
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/common:DictKeyInfo'
    ]
)
strict_cc_library(
    name='TimeStrategyTruncateMetaReader',
    deps=[
        ':TruncateMetaReader', '//aios/autil:log',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/common:DictKeyInfo'
    ]
)
strict_cc_library(
    name='TruncateTrigger',
    srcs=[],
    deps=[
        ':TruncateMetaReader', '//aios/autil:log',
        '//aios/storage/indexlib/index/common:DictKeyInfo',
        '//aios/storage/indexlib/index/inverted_index:Types'
    ]
)
strict_cc_library(
    name='TruncateMetaTrigger',
    deps=[':TruncateMetaReader', ':TruncateTrigger']
)
strict_cc_library(
    name='DefaultTruncateTrigger',
    srcs=[],
    deps=[':TruncateMetaReader', ':TruncateTrigger']
)
strict_cc_library(
    name='TruncateTriggerCreator',
    deps=[
        ':DefaultTruncateTrigger', ':TruncateMetaTrigger', ':TruncateTrigger',
        '//aios/storage/indexlib/index/inverted_index:Common',
        '//aios/storage/indexlib/index/inverted_index:Constant',
        '//aios/storage/indexlib/index/inverted_index/config:TruncateIndexProperty'
    ]
)
strict_cc_library(
    name='BucketMap',
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system:interface',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResource'
    ]
)
strict_cc_library(
    name='BucketMapCreator',
    deps=[
        ':BucketMap', ':SortWorkItem', ':TruncateAttributeReaderCreator',
        '//aios/autil:log', '//aios/autil:thread',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/index:DocMapper',
        '//aios/storage/indexlib/index/attribute:indexer',
        '//aios/storage/indexlib/index/inverted_index/config:TruncateOptionConfig',
        '//aios/storage/indexlib/index/inverted_index/config:TruncateProfile'
    ]
)
strict_cc_library(
    name='TruncatePostingIterator',
    deps=[
        '//aios/storage/indexlib/index/inverted_index:PostingIterator',
        '//aios/storage/indexlib/index/inverted_index/format:TermMeta'
    ]
)
strict_cc_library(
    name='TruncatePostingIteratorCreator',
    deps=[
        ':TruncatePostingIterator',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/index/inverted_index:BufferedPostingIterator',
        '//aios/storage/indexlib/index/inverted_index:PostingIterator',
        '//aios/storage/indexlib/index/inverted_index:SegmentPosting',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:BitmapPostingIterator',
        '//aios/storage/indexlib/index/inverted_index/format:TermMeta'
    ]
)
strict_cc_library(
    name='TruncateIndexWriter',
    srcs=[],
    deps=[
        ':TruncateTrigger', '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system:interface',
        '//aios/storage/indexlib/framework:SegmentMeta',
        '//aios/storage/indexlib/index/common:DictKeyInfo',
        '//aios/storage/indexlib/index/inverted_index:PostingIterator'
    ]
)
strict_cc_library(
    name='SingleTruncateIndexWriter',
    deps=[
        ':BucketMap', ':DocCollector', ':DocInfoAllocator', ':IEvaluator',
        ':Reference', ':TruncateIndexWriter', ':TruncatePostingIterator',
        '//aios/storage/indexlib/framework:SegmentMeta',
        '//aios/storage/indexlib/index/inverted_index:IndexOutputSegmentResource',
        '//aios/storage/indexlib/index/inverted_index:MultiSegmentPostingWriter',
        '//aios/storage/indexlib/index/inverted_index/format:IndexFormatOption'
    ]
)
strict_cc_library(
    name='MultiTruncateIndexWriter',
    deps=[
        ':MultiTruncateWriterScheduler', ':SimpleTruncateWriterScheduler',
        ':SingleTruncateIndexWriter', ':TruncateWorkItem'
    ]
)
strict_cc_library(
    name='TruncateWorkItem',
    deps=[
        ':SingleTruncateIndexWriter',
        '//aios/storage/indexlib/index/inverted_index:PostingIterator'
    ]
)
strict_cc_library(
    name='TruncateMetaFileReaderCreator',
    deps=[
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system:interface'
    ]
)
strict_cc_library(
    name='TruncateIndexWriterCreator',
    deps=[
        ':BucketMap', ':DocCollectorCreator', ':DocInfoAllocator',
        ':EvaluatorCreator', ':MultiTruncateIndexWriter',
        ':SingleTruncateIndexWriter', ':TimeStrategyTruncateMetaReader',
        ':TruncateAttributeReaderCreator', ':TruncateIndexWriter',
        ':TruncateMetaFileReaderCreator', ':TruncateTriggerCreator',
        '//aios/autil:log', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system:interface',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResource',
        '//aios/storage/indexlib/index/inverted_index/format:TermMeta'
    ]
)
strict_cc_library(
    name='DocInfo',
    srcs=[],
    deps=['//aios/autil:string_helper', '//aios/storage/indexlib/base:Types']
)
strict_cc_library(
    name='Reference',
    srcs=[],
    deps=[':DocInfo', '//aios/storage/indexlib/base:FieldType']
)
strict_cc_library(name='ReferenceTyped', srcs=[], deps=[':Reference'])
strict_cc_library(
    name='IEvaluator',
    srcs=[],
    deps=[
        ':DocInfo',
        '//aios/storage/indexlib/index/inverted_index:PostingIterator',
        '//aios/storage/indexlib/index/inverted_index/format:TermMeta'
    ]
)
strict_cc_library(
    name='AttributeEvaluator',
    srcs=[],
    deps=[
        ':IEvaluator', ':ReferenceTyped',
        '//aios/storage/indexlib/index/attribute:indexer'
    ]
)
strict_cc_library(name='MultiAttributeEvaluator', deps=[':IEvaluator'])
strict_cc_library(
    name='DocPayloadEvaluator', deps=[':IEvaluator', ':ReferenceTyped']
)
strict_cc_library(
    name='DocInfoAllocator',
    deps=[
        ':DocInfo', ':ReferenceTyped', '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/util:mmap_pool'
    ]
)
strict_cc_library(
    name='EvaluatorCreator',
    deps=[
        ':AttributeEvaluator', ':DocInfoAllocator', ':DocPayloadEvaluator',
        ':IEvaluator', ':MultiAttributeEvaluator', ':ReferenceTyped',
        ':TruncateAttributeReaderCreator',
        '//aios/storage/indexlib/index/inverted_index/config:DiversityConstrain',
        '//aios/storage/indexlib/index/inverted_index/config:TruncateIndexProperty',
        '//aios/storage/indexlib/index/inverted_index/config:TruncateProfile',
        '//aios/storage/indexlib/util:ClassTypedFactory'
    ]
)
strict_cc_library(
    name='DocDistinctor',
    srcs=[],
    deps=[':TruncateAttributeReader', '//aios/storage/indexlib/util:HashMap']
)
strict_cc_library(
    name='ITruncateWriterScheduler',
    srcs=[],
    deps=[
        '//aios/autil:log', '//aios/autil:thread',
        '//aios/storage/indexlib/base:Status'
    ]
)
strict_cc_library(
    name='SimpleTruncateWriterScheduler', deps=[':ITruncateWriterScheduler']
)
strict_cc_library(
    name='MultiTruncateWriterScheduler', deps=[':ITruncateWriterScheduler']
)
strict_cc_library(
    name='IDocFilterProcessor',
    srcs=[],
    deps=[
        ':TruncateAttributeReader', ':TruncateMetaReader',
        '//aios/storage/indexlib/index/inverted_index:PostingIterator',
        '//aios/storage/indexlib/index/inverted_index/config:DiversityConstrain',
        '//aios/storage/indexlib/index/inverted_index/format:TermMeta'
    ]
)
strict_cc_library(
    name='DocFilterProcessorTyped', srcs=[], deps=[':IDocFilterProcessor']
)
strict_cc_library(
    name='DocPayloadFilterProcessor',
    deps=[':DocInfoAllocator', ':IDocFilterProcessor', ':IEvaluator']
)
strict_cc_library(
    name='BucketVectorAllocator',
    srcs=[],
    deps=['//aios/storage/indexlib/base:Types']
)
strict_cc_library(
    name='DocCollector',
    srcs=[],
    deps=[':BucketVectorAllocator', ':DocDistinctor', ':IDocFilterProcessor']
)
strict_cc_library(
    name='Comparator',
    srcs=[],
    deps=[':DocInfo', ':Reference', ':ReferenceTyped']
)
strict_cc_library(
    name='MultiComparator', deps=[':Comparator', '//aios/autil:log']
)
strict_cc_library(name='NoSortTruncateCollector', deps=[':DocCollector'])
strict_cc_library(
    name='SortTruncateCollector',
    deps=[
        ':BucketMap', ':DocCollector', ':TruncateAttributeReaderCreator',
        '//aios/storage/indexlib/index/inverted_index/config:TruncateProfile',
        '//aios/storage/indexlib/util:MathUtil'
    ]
)
strict_cc_library(
    name='DocCollectorCreator',
    deps=[
        ':BucketMapCreator', ':Comparator', ':DocCollector', ':DocDistinctor',
        ':DocFilterProcessorTyped', ':DocInfoAllocator',
        ':DocPayloadFilterProcessor', ':IDocFilterProcessor',
        ':MultiComparator', ':NoSortTruncateCollector',
        ':SortTruncateCollector', ':TruncateAttributeReaderCreator',
        ':TruncateMetaReader', '//aios/storage/indexlib/util:ClassTypedFactory'
    ]
)
