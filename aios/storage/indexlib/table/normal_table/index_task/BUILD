load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='document_reclaim',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[
        ':PrepareIndexReclaimParamOperation',
        '//aios/storage/indexlib/table/normal_table/index_task/document_reclaim:IndexReclaimerParam'
    ]
)
strict_cc_library(
    name='NormalTableTaskPlanCreator',
    deps=[
        ':NormalTableAlterTablePlanCreator', ':NormalTableCompactPlanCreator',
        ':NormalTableReclaimPlanCreator', ':NormalTableSplitPlanCreator',
        ':NormalTableTaskOperationCreator', '//aios/autil:log',
        '//aios/storage/indexlib/table/index_task:ComplexIndexTaskPlanCreator'
    ]
)
strict_cc_library(
    name='NormalTableTaskOperationCreator',
    deps=[
        ':AttributeIndexMergeOperation', ':BucketMapOperation',
        ':DeletionMapIndexMergeOperation', ':DropOpLogOperation',
        ':InvertedIndexMergeOperation',
        ':MultiShardInvertedIndexMergeOperation',
        ':NormalTableAddIndexOperation', ':NormalTableReclaimOperation',
        ':NormalTableResourceCreator', ':OpLog2PatchOperation',
        ':PackAttributeIndexMergeOperation', ':ReclaimMapOperation',
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/framework/index_task:IIndexOperationCreator',
        '//aios/storage/indexlib/table/index_task/add_index:PatchSegmentMoveOperation',
        '//aios/storage/indexlib/table/index_task/merger:CommonTaskOperationCreator'
    ]
)
strict_cc_library(
    name='NormalTableAlterTablePlanCreator',
    deps=[
        ':DropOpLogOperation', ':NormalTableAddIndexOperation',
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/table/index_task:IndexTaskConstant',
        '//aios/storage/indexlib/table/index_task:SimpleIndexTaskPlanCreator',
        '//aios/storage/indexlib/table/index_task/add_index:PatchSegmentMoveOperation',
        '//aios/storage/indexlib/table/index_task/merger:EndMergeTaskOperation',
        '//aios/storage/indexlib/table/index_task/merger:FenceDirDeleteOperation',
        '//aios/storage/indexlib/table/index_task/merger:MergedVersionCommitOperation'
    ]
)
strict_cc_library(
    name='NormalTableReclaimPlanCreator',
    deps=[
        ':Common', ':NormalTableReclaimOperation',
        ':PrepareIndexReclaimParamOperation', '//aios/autil:NoCopyable',
        '//aios/autil:log', '//aios/storage/indexlib/config:IndexTaskConfig',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/index/attribute:patch',
        '//aios/storage/indexlib/index/inverted_index:patch',
        '//aios/storage/indexlib/table/index_task:IndexTaskConstant',
        '//aios/storage/indexlib/table/index_task:SimpleIndexTaskPlanCreator',
        '//aios/storage/indexlib/table/index_task/merger:FenceDirDeleteOperation',
        '//aios/storage/indexlib/table/index_task/merger:MergedVersionCommitOperation',
        '//aios/storage/indexlib/table/index_task/merger:PathMoveOperation'
    ]
)
strict_cc_library(
    name='NormalTableCompactPlanCreator',
    deps=[
        ':NormalTableMergeDescriptionCreator', '//aios/autil:NoCopyable',
        '//aios/future_lite', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/config:MergeConfig',
        '//aios/storage/indexlib/config:OfflineConfig',
        '//aios/storage/indexlib/config:TabletOptions',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/framework:Version',
        '//aios/storage/indexlib/framework/index_task:IndexTaskContext',
        '//aios/storage/indexlib/framework/index_task:IndexTaskPlan',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResource',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResourceManager',
        '//aios/storage/indexlib/table/index_task:IndexTaskConstant',
        '//aios/storage/indexlib/table/index_task:SimpleIndexTaskPlanCreator',
        '//aios/storage/indexlib/table/index_task/merger:MergeStrategy',
        '//aios/storage/indexlib/table/index_task/merger:RealtimeMergeStrategy',
        '//aios/storage/indexlib/table/index_task/merger:SpecificSegmentsMergeStrategy',
        '//aios/storage/indexlib/table/normal_table:NormalSchemaResolver',
        '//aios/storage/indexlib/table/normal_table/index_task:NormalTableResourceCreator',
        '//aios/storage/indexlib/table/normal_table/index_task/merger:AdaptiveMergeStrategy',
        '//aios/storage/indexlib/table/normal_table/index_task/merger:BalanceTreeMergeStrategy',
        '//aios/storage/indexlib/table/normal_table/index_task/merger:CombinedMergeStrategy',
        '//aios/storage/indexlib/table/normal_table/index_task/merger:NormalTableMergeStrategyUtil',
        '//aios/storage/indexlib/table/normal_table/index_task/merger:OptimizeMergeStrategy',
        '//aios/storage/indexlib/table/normal_table/index_task/merger:PriorityQueueMergeStrategy'
    ]
)
strict_cc_library(
    name='NormalTableResourceCreator',
    deps=[
        ':SingleSegmentDocumentGroupSelector',
        '//aios/storage/indexlib/framework/index_task:IIndexTaskResourceCreator',
        '//aios/storage/indexlib/index:DocMapper',
        '//aios/storage/indexlib/index/inverted_index/truncate:BucketMap',
        '//aios/storage/indexlib/table/index_task/merger:MergePlan',
        '//aios/storage/indexlib/table/normal_table/index_task:ReclaimMap',
        '//aios/storage/indexlib/table/normal_table/index_task:SortedReclaimMap'
    ]
)
strict_cc_library(
    name='ReclaimMap',
    deps=[
        ':SegmentMapper', '//aios/autil:log',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResource',
        '//aios/storage/indexlib/index:DocMapper',
        '//aios/storage/indexlib/index/deletionmap:config',
        '//aios/storage/indexlib/index/deletionmap:merger',
        '//aios/storage/indexlib/index/deletionmap:reader',
        '//aios/storage/indexlib/table/index_task/merger:SegmentMergePlan'
    ]
)
strict_cc_library(
    name='SortedReclaimMap',
    hdrs=['SortedReclaimMap.h', 'WeightedDocIterator.h'],
    deps=[
        ':ReclaimMap', ':SegmentMergeInfo', '//aios/autil:log',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResource',
        '//aios/storage/indexlib/index:DocMapper',
        '//aios/storage/indexlib/index/deletionmap:config',
        '//aios/storage/indexlib/index/deletionmap:merger',
        '//aios/storage/indexlib/index/inverted_index/truncate:AttributeEvaluator',
        '//aios/storage/indexlib/index/inverted_index/truncate:Comparator',
        '//aios/storage/indexlib/index/inverted_index/truncate:DocInfoAllocator',
        '//aios/storage/indexlib/index/inverted_index/truncate:IEvaluator',
        '//aios/storage/indexlib/index/inverted_index/truncate:MultiAttributeEvaluator',
        '//aios/storage/indexlib/index/inverted_index/truncate:MultiComparator',
        '//aios/storage/indexlib/index/inverted_index/truncate:Reference',
        '//aios/storage/indexlib/table/index_task/merger:SegmentMergePlan',
        '//aios/storage/indexlib/util:ClassTypedFactory'
    ]
)
strict_cc_library(
    name='AttributeIndexMergeOperation',
    deps=[
        ':Common', ':OpLog2PatchOperation', ':PatchFileFinderUtil',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework/index_task:IndexOperationDescription',
        '//aios/storage/indexlib/index/attribute:merger',
        '//aios/storage/indexlib/index/attribute:patch',
        '//aios/storage/indexlib/table/index_task/merger:IndexMergeOperation'
    ]
)
strict_cc_library(
    name='PackAttributeIndexMergeOperation',
    deps=[
        ':Common', '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework/index_task:IndexOperationDescription',
        '//aios/storage/indexlib/table/index_task/merger:IndexMergeOperation'
    ]
)
strict_cc_library(
    name='PrepareIndexReclaimParamOperation',
    deps=[
        '//aios/storage/indexlib/framework/index_task:IndexOperation',
        '//aios/storage/indexlib/framework/index_task:IndexOperationDescription',
        '//aios/storage/indexlib/framework/index_task:IndexTaskContext',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResourceManager',
        '//aios/storage/indexlib/util:task'
    ]
)
strict_cc_library(
    name='NormalTableReclaimOperation',
    deps=[
        ':PatchedDeletionMapLoader', ':PrepareIndexReclaimParamOperation',
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/framework/index_task:IndexOperation',
        '//aios/storage/indexlib/framework/index_task:IndexOperationDescription',
        '//aios/storage/indexlib/index/attribute:reader',
        '//aios/storage/indexlib/index/deletionmap:modifier',
        '//aios/storage/indexlib/table/index_task:IndexTaskConstant',
        '//aios/storage/indexlib/table/normal_table:NormalMemSegment',
        '//aios/storage/indexlib/table/normal_table/index_task:Common',
        '//aios/storage/indexlib/table/normal_table/index_task/document_reclaim:IndexReclaimer',
        '//aios/storage/indexlib/table/normal_table/index_task/document_reclaim:IndexReclaimerCreator',
        '//aios/storage/indexlib/util:Clock'
    ]
)
strict_cc_library(
    name='InvertedIndexMergeOperation',
    deps=[
        ':Common', ':OpLog2PatchOperation', ':PatchFileFinderUtil',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework/index_task:IndexOperationDescription',
        '//aios/storage/indexlib/index/inverted_index:merger',
        '//aios/storage/indexlib/index/inverted_index/truncate:TruncateAttributeReaderCreator',
        '//aios/storage/indexlib/index/inverted_index/truncate:TruncateMetaFileReaderCreator',
        '//aios/storage/indexlib/table/index_task/merger:IndexMergeOperation'
    ]
)
strict_cc_library(
    name='MultiShardInvertedIndexMergeOperation',
    deps=[
        ':Common', ':InvertedIndexMergeOperation',
        '//aios/storage/indexlib/framework/index_task:IndexOperationDescription'
    ]
)
strict_cc_library(
    name='DeletionMapIndexMergeOperation',
    deps=[
        ':Common', ':PatchedDeletionMapLoader',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/deletionmap:merger',
        '//aios/storage/indexlib/table/index_task/merger:IndexMergeOperation'
    ]
)
strict_cc_library(
    name='PatchedDeletionMapLoader',
    deps=[
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/index/deletionmap:modifier'
    ]
)
strict_cc_library(
    name='OpLog2PatchOperation',
    deps=[
        ':Common', ':PatchedDeletionMapLoader', '//aios/autil:log',
        '//aios/storage/indexlib/config:TabletOptions',
        '//aios/storage/indexlib/config:schema',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/framework/index_task:IndexOperation',
        '//aios/storage/indexlib/framework/index_task:IndexOperationDescription',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResourceManager',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/index/attribute:patch',
        '//aios/storage/indexlib/index/common/patch:PatchWriter',
        '//aios/storage/indexlib/index/deletionmap:modifier',
        '//aios/storage/indexlib/index/inverted_index/patch:InvertedIndexPatchWriter',
        '//aios/storage/indexlib/index/operation_log:reader',
        '//aios/storage/indexlib/index/primary_key:reader',
        '//aios/storage/indexlib/table/index_task:IndexTaskConstant',
        '//aios/storage/indexlib/table/index_task/merger:MergePlan'
    ]
)
strict_cc_library(
    name='DropOpLogOperation',
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/config:schema',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/framework/index_task:IndexOperation',
        '//aios/storage/indexlib/framework/index_task:IndexOperationDescription',
        '//aios/storage/indexlib/index/attribute:constants',
        '//aios/storage/indexlib/index/inverted_index:constants',
        '//aios/storage/indexlib/index/operation_log:config'
    ]
)
strict_cc_library(
    name='ReclaimMapOperation',
    deps=[
        ':Common', ':NormalTableResourceCreator', ':PatchedDeletionMapLoader',
        ':ReclaimMap', ':SingleSegmentDocumentGroupSelector',
        ':SortedReclaimMap', '//aios/autil:log',
        '//aios/storage/indexlib/framework/index_task:IndexOperation',
        '//aios/storage/indexlib/framework/index_task:IndexOperationDescription',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResourceManager',
        '//aios/storage/indexlib/index/deletionmap:constants',
        '//aios/storage/indexlib/table/index_task:IndexTaskConstant',
        '//aios/storage/indexlib/table/index_task/merger:MergePlan',
        '//aios/storage/indexlib/table/index_task/merger:SegmentMergePlan',
        '//aios/storage/indexlib/table/normal_table:NormalSchemaResolver',
        '//aios/storage/indexlib/table/normal_table/config:SegmentGroupConfig'
    ]
)
strict_cc_library(
    name='BucketMapOperation',
    deps=[
        ':Common', ':NormalTableResourceCreator', '//aios/autil:log',
        '//aios/storage/indexlib/framework/index_task:IndexOperation',
        '//aios/storage/indexlib/framework/index_task:IndexOperationDescription',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResourceManager',
        '//aios/storage/indexlib/index:DocMapper',
        '//aios/storage/indexlib/index/deletionmap:constants',
        '//aios/storage/indexlib/index/inverted_index/config:TruncateOptionConfig',
        '//aios/storage/indexlib/index/inverted_index/truncate:BucketMap',
        '//aios/storage/indexlib/index/inverted_index/truncate:BucketMapCreator',
        '//aios/storage/indexlib/index/inverted_index/truncate:TruncateAttributeReaderCreator',
        '//aios/storage/indexlib/table/index_task:IndexTaskConstant',
        '//aios/storage/indexlib/table/index_task/merger:MergePlan',
        '//aios/storage/indexlib/table/index_task/merger:SegmentMergePlan'
    ]
)
strict_cc_library(
    name='NormalTableMergeDescriptionCreator',
    deps=[
        ':BucketMapOperation', ':NormalTableResourceCreator',
        ':OpLog2PatchOperation', ':ReclaimMapOperation',
        '//aios/storage/indexlib/framework/index_task:BasicDefs',
        '//aios/storage/indexlib/index:DocMapper',
        '//aios/storage/indexlib/index/inverted_index:constants',
        '//aios/storage/indexlib/table/index_task/merger:CommonMergeDescriptionCreator',
        '//aios/storage/indexlib/table/index_task/merger:MergeStrategyDefine',
        '//aios/storage/indexlib/table/normal_table:NormalSchemaResolver'
    ]
)
strict_cc_library(
    name='Common',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/index/attribute:constants',
        '//aios/storage/indexlib/index/deletionmap:constants',
        '//aios/storage/indexlib/index/operation_log:constants',
        '//aios/storage/indexlib/index/pack_attribute:constants',
        '//aios/storage/indexlib/index/primary_key:constants'
    ]
)
strict_cc_library(
    name='SingleSegmentDocumentGroupSelector',
    deps=[
        '//aios/autil:NoCopyable', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/config:schema',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/index/attribute/expression:evaluator',
        '//aios/storage/indexlib/table/normal_table:Common',
        '//aios/storage/indexlib/table/normal_table/config:SegmentGroupConfig'
    ]
)
strict_cc_library(
    name='NormalTableSplitPlanCreator',
    deps=[
        ':NormalTableCompactPlanCreator',
        '//aios/storage/indexlib/table/normal_table/index_task/merger:SplitStrategy'
    ]
)
strict_cc_library(
    name='NormalTableAddIndexOperation',
    deps=[
        '//aios/storage/indexlib/analyzer',
        '//aios/storage/indexlib/document/extractor/plain:DocumentInfoExtractorFactory',
        '//aios/storage/indexlib/document/normal:NormalDocumentParser',
        '//aios/storage/indexlib/document/raw_document:DefaultRawDocument',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/framework/index_task:IndexOperation',
        '//aios/storage/indexlib/framework/index_task:IndexOperationDescription',
        '//aios/storage/indexlib/index:IndexFactoryCreator',
        '//aios/storage/indexlib/index:IndexerParameter',
        '//aios/storage/indexlib/table/normal_table:NormalTabletDocIterator',
        '//aios/storage/indexlib/table/normal_table:NormalTabletExportLoader',
        '//aios/storage/indexlib/util:simple_pool'
    ]
)
strict_cc_library(
    name='PatchFileFinderUtil',
    deps=[
        ':Common', '//aios/autil:log',
        '//aios/storage/indexlib/framework/index_task:IndexOperationDescription',
        '//aios/storage/indexlib/framework/index_task:IndexTaskContext',
        '//aios/storage/indexlib/index/common/patch:SrcDestPatchFileFinder',
        '//aios/storage/indexlib/table/index_task:IndexTaskConstant'
    ]
)
strict_cc_library(
    name='SegmentMapper', srcs=[], deps=['//aios/storage/indexlib/base:Types']
)
strict_cc_library(
    name='SegmentMergeInfo',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/framework:SegmentInfo',
        '//aios/storage/indexlib/framework:SegmentMetrics'
    ]
)
