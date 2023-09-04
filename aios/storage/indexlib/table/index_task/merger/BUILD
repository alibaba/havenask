load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='SegmentMergePlan',
    deps=['//aios/autil:json', '//aios/storage/indexlib/framework:TabletData']
)
strict_cc_library(
    name='MergePlan',
    deps=[
        ':SegmentMergePlan', '//aios/autil:json',
        '//aios/storage/indexlib/base:NoExceptionWrapper',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework:Version',
        '//aios/storage/indexlib/framework/index_task:IndexTaskContext',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResource',
        '//aios/storage/indexlib/table/index_task:IndexTaskConstant'
    ]
)
strict_cc_library(name='MergeStrategy', deps=[':MergePlan'])
strict_cc_library(
    name='CommonTaskOperationCreator',
    deps=[
        ':EndMergeTaskOperation', ':FenceDirDeleteOperation',
        ':IndexMergeOperation', ':MergedSegmentMoveOperation',
        ':MergedVersionCommitOperation', ':PathMoveOperation',
        '//aios/storage/indexlib/framework/index_task:IIndexOperationCreator',
        '//aios/storage/indexlib/framework/index_task:IndexOperation',
        '//aios/storage/indexlib/framework/index_task:IndexOperationDescription'
    ]
)
strict_cc_library(
    name='CommonMergeDescriptionCreator',
    deps=[
        ':EndMergeTaskOperation', ':FenceDirDeleteOperation',
        ':IndexMergeOperation', ':MergedSegmentMoveOperation',
        ':MergedVersionCommitOperation',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/framework/index_task:IndexOperationDescription',
        '//aios/storage/indexlib/table/index_task:IndexTaskConstant'
    ]
)
strict_cc_library(name='MergeStrategyDefine', deps=['//aios/autil:log'])
strict_cc_library(
    name='ShardBasedMergeStrategy',
    deps=[
        ':MergePlan', ':MergeStrategy', ':MergeStrategyDefine',
        '//aios/autil:log', '//aios/storage/indexlib/config:MergeConfig',
        '//aios/storage/indexlib/config:OfflineConfig',
        '//aios/storage/indexlib/config:TabletOptions',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/framework/index_task:IndexTaskContext',
        '//aios/storage/indexlib/index/deletionmap:constants'
    ]
)
strict_cc_library(
    name='MultiShardIndexMergeOperation',
    deps=[
        ':IndexMergeOperation', ':MergePlan', ':MergeStrategy',
        ':MergeStrategyDefine', '//aios/autil:log',
        '//aios/storage/indexlib/config:MergeConfig',
        '//aios/storage/indexlib/config:OfflineConfig',
        '//aios/storage/indexlib/config:TabletOptions',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/framework/index_task:IndexTaskContext',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/table/plain:MultiShardDiskSegment'
    ]
)
strict_cc_library(
    name='RealtimeMergeStrategy',
    srcs=['RealtimeMergeStrategy.cpp'],
    hdrs=['RealtimeMergeStrategy.h'],
    deps=[
        ':MergePlan', ':MergeStrategy', ':MergeStrategyDefine',
        '//aios/autil:log', '//aios/storage/indexlib/config:MergeConfig',
        '//aios/storage/indexlib/config:OfflineConfig',
        '//aios/storage/indexlib/config:TabletOptions',
        '//aios/storage/indexlib/framework:LevelInfo',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/framework/index_task:IndexTaskContext',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/index/deletionmap:constants'
    ]
)
strict_cc_library(
    name='SpecificSegmentsMergeStrategy',
    deps=[
        ':MergePlan', ':MergeStrategy', ':MergeStrategyDefine',
        '//aios/autil:log', '//aios/storage/indexlib/config:MergeConfig',
        '//aios/storage/indexlib/config:OfflineConfig',
        '//aios/storage/indexlib/config:TabletOptions',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/framework/index_task:IndexTaskContext',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/index/deletionmap:constants'
    ]
)
strict_cc_library(
    name='IndexMergeOperation',
    deps=[
        ':MergePlan', '//aios/autil:log',
        '//aios/storage/indexlib/base:PathUtil',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/framework/index_task:IndexOperation',
        '//aios/storage/indexlib/framework/index_task:IndexOperationDescription',
        '//aios/storage/indexlib/framework/index_task:IndexTaskContext',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResourceManager',
        '//aios/storage/indexlib/index:IIndexFactory',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/index:IndexFactoryCreator',
        '//aios/storage/indexlib/table/index_task:IndexTaskConstant'
    ]
)
strict_cc_library(
    name='MergedVersionCommitOperation',
    deps=[
        ':MergePlan', '//aios/autil:log',
        '//aios/storage/indexlib/config:TabletOptions',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework:VersionCommitter',
        '//aios/storage/indexlib/framework:VersionValidator',
        '//aios/storage/indexlib/framework:version_cleaner',
        '//aios/storage/indexlib/framework/cleaner:DropIndexCleaner',
        '//aios/storage/indexlib/framework/index_task:IndexOperation',
        '//aios/storage/indexlib/framework/index_task:IndexOperationDescription',
        '//aios/storage/indexlib/framework/index_task:IndexTaskContext',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResourceManager',
        '//aios/storage/indexlib/index/inverted_index/config:TruncateIndexNameMapper',
        '//aios/storage/indexlib/table/index_task:VersionResource'
    ]
)
strict_cc_library(
    name='MergedSegmentMoveOperation',
    deps=[
        ':MergePlan', '//aios/autil:log', '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework/index_task:IndexOperation',
        '//aios/storage/indexlib/framework/index_task:IndexOperationDescription',
        '//aios/storage/indexlib/framework/index_task:IndexTaskContext',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResourceManager',
        '//aios/storage/indexlib/index/inverted_index/config:TruncateIndexNameMapper',
        '//aios/storage/indexlib/table/index_task:IndexTaskConstant'
    ]
)
strict_cc_library(
    name='PathMoveOperation',
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework/index_task:IndexOperation',
        '//aios/storage/indexlib/framework/index_task:IndexOperationDescription',
        '//aios/storage/indexlib/framework/index_task:IndexTaskContext'
    ]
)
strict_cc_library(
    name='EndMergeTaskOperation',
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/framework:Version',
        '//aios/storage/indexlib/framework:VersionLoader',
        '//aios/storage/indexlib/framework/index_task:IndexOperation',
        '//aios/storage/indexlib/framework/index_task:IndexOperationDescription',
        '//aios/storage/indexlib/framework/index_task:IndexTaskContext',
        '//aios/storage/indexlib/framework/index_task:MergeTaskDefine',
        '//aios/storage/indexlib/table/index_task:IndexTaskConstant'
    ]
)
strict_cc_library(
    name='FenceDirDeleteOperation',
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework/index_task:IndexOperation',
        '//aios/storage/indexlib/framework/index_task:IndexOperationDescription',
        '//aios/storage/indexlib/framework/index_task:IndexTaskContext'
    ]
)
