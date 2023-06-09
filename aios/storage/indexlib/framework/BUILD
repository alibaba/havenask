load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
indexlib_cc_library(
    name='tablet',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[
        ':Tablet', ':TabletCreator', ':TabletDeployer', ':TabletPortalManager'
    ]
)
indexlib_cc_library(
    name='id_generator',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':IdGenerator']
)
indexlib_cc_library(
    name='version_cleaner',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':VersionCleaner']
)
indexlib_cc_library(
    name='tablet_doc_iterator',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':ITabletDocIterator']
)
indexlib_cc_library(
    name='tablet_factory',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':ITabletFactory']
)
indexlib_cc_library(
    name='tablet_schema_loader',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':TabletSchemaLoader']
)
indexlib_cc_library(name='IndexRoot', srcs=[])
indexlib_cc_library(name='TabletId')
indexlib_cc_library(name='DumpParams', srcs=[])
indexlib_cc_library(
    name='ITabletDocIterator', srcs=[], hdrs=['ITabletDocIterator.h']
)
indexlib_cc_library(
    name='ITabletValidator', srcs=[], hdrs=['ITabletValidator.h']
)
indexlib_cc_library(name='ITabletExporter', srcs=[], hdrs=['ITabletExporter.h'])
indexlib_cc_library(
    name='TabletReaderContainer',
    deps=[
        '//aios/storage/indexlib/framework:ITabletReader',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/framework:VersionDeployDescription'
    ]
)
indexlib_cc_library(
    name='LevelInfo',
    deps=[
        '//aios/autil:json', '//aios/autil:string_helper',
        '//aios/storage/indexlib/base:Constant',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/util:ShardUtil'
    ]
)
indexlib_cc_library(
    name='SegmentTopologyInfo', deps=[':LevelInfo', '//aios/autil:json']
)
indexlib_cc_library(
    name='SegmentDescriptions',
    deps=[
        ':LevelInfo', ':SegmentStatistics', ':SegmentTopologyInfo',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:constants'
    ]
)
indexlib_cc_library(
    name='TabletDumper',
    deps=[
        ':SegmentDumper', ':TabletCommitter', '//aios/future_lite',
        '//aios/storage/indexlib/config:TabletOptions'
    ]
)
indexlib_cc_library(
    name='TabletSchemaManager',
    deps=[
        ':ITabletFactory', ':TabletSchemaLoader', ':Version',
        '//aios/storage/indexlib/config:TabletOptions',
        '//aios/storage/indexlib/file_system'
    ]
)
indexlib_cc_library(
    name='TabletCenter',
    deps=[
        ':ITablet', ':TabletInfos', '//aios/alog', '//aios/autil:env_util',
        '//aios/autil:thread', '//aios/storage/indexlib/config:schema',
        '//aios/storage/indexlib/util:Singleton'
    ]
)
indexlib_cc_library(
    name='TabletCommitter',
    deps=[
        ':CommitOptions', ':IdGenerator', ':SegmentMeta', ':TabletData',
        ':Version', ':VersionCommitter', ':VersionImporter', ':VersionMerger',
        '//aios/alog', '//aios/autil:time',
        '//aios/storage/indexlib/base:NoExceptionWrapper',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework/cleaner:DropIndexCleaner'
    ]
)
indexlib_cc_library(
    name='SegmentDumper',
    deps=[
        ':Locator', ':MemSegment', ':MetricsWrapper', ':SegmentDumpItem',
        ':Version', '//aios/future_lite',
        '//aios/storage/indexlib/base:NoExceptionWrapper'
    ]
)
indexlib_cc_library(
    name='SegmentDumpItem',
    srcs=[],
    deps=[':TabletData', '//aios/storage/indexlib/config:TabletOptions']
)
indexlib_cc_library(
    name='SegmentMeta',
    deps=[
        ':SegmentInfo', ':SegmentMetrics', '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/file_system'
    ]
)
indexlib_cc_library(
    name='ITabletLoader', srcs=[], deps=['//aios/storage/indexlib/base:Status']
)
indexlib_cc_library(
    name='TabletLoader',
    deps=[
        ':DiskSegment', ':ITabletLoader', ':Segment', ':SegmentMeta',
        ':TabletData', ':Version', '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/config:schema'
    ]
)
indexlib_cc_library(
    name='TabletInfos',
    deps=[
        ':IndexRoot', ':Locator', ':TabletId', ':Version',
        '//aios/storage/indexlib/util/counter'
    ]
)
indexlib_cc_library(
    name='Locator',
    deps=[
        '//aios/alog', '//aios/autil:json',
        '//aios/storage/indexlib/base:Progress',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants'
    ]
)
indexlib_cc_library(
    name='BuildResource', srcs=[], deps=['//aios/kmonitor:kmonitor_client_cpp']
)
indexlib_cc_library(
    name='ReadResource', srcs=[], deps=['//aios/kmonitor:kmonitor_client_cpp']
)
indexlib_cc_library(
    name='TabletData',
    deps=[
        ':Segment', ':TabletDataInfo', ':TabletDataSchemaGroup', ':Version',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/config:schema',
        '//aios/storage/indexlib/framework:MemSegment',
        '//aios/storage/indexlib/framework:ResourceMap'
    ]
)
indexlib_cc_library(
    name='TabletResource',
    srcs=[],
    deps=[
        ':TabletId', '//aios/future_lite', '//aios/kmonitor:kmonitor_client_cpp'
    ]
)
indexlib_cc_library(
    name='Version',
    deps=[
        ':Locator', ':SegmentDescriptions', ':VersionLine', '//aios/alog',
        '//aios/autil:json', '//aios/autil:string_helper',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework/index_task:IndexTaskHistory'
    ]
)
indexlib_cc_library(
    name='MemSegment',
    srcs=[],
    deps=[':BuildResource', ':Segment', '//aios/storage/indexlib/base:Status']
)
indexlib_cc_library(
    name='ITabletReader', srcs=[], deps=['//aios/storage/indexlib/base:Status']
)
indexlib_cc_library(
    name='ITabletPortal', srcs=[], deps=['//aios/storage/indexlib/base:Status']
)
indexlib_cc_library(
    name='TabletPortalBase',
    deps=[
        ':ITabletPortal', ':Tablet', ':TabletInfos', '//aios/alog',
        '//aios/autil:json', '//aios/autil:string_helper',
        '//aios/storage/indexlib/base:Status'
    ]
)
indexlib_cc_library(
    name='HttpTabletPortal',
    deps=[
        ':TabletPortalBase', '//aios/storage/indexlib/util:headers',
        '//aios/storage/indexlib/util:key_value_map'
    ]
)
indexlib_cc_library(name='TabletPortalManager', deps=[':HttpTabletPortal'])
indexlib_cc_library(name='OpenOptions', deps=['//aios/autil:log'])
indexlib_cc_library(
    name='CommitOptions',
    srcs=[],
    deps=['//aios/storage/indexlib/base:constants']
)
indexlib_cc_library(name='ImportOptions', srcs=[])
indexlib_cc_library(
    name='Segment',
    srcs=[],
    deps=[
        ':SegmentDescriptions', ':SegmentMeta', '//aios/autil:NoCopyable',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/file_system'
    ]
)
indexlib_cc_library(
    name='DiskSegment', srcs=[], deps=[':BuildResource', ':Segment']
)
indexlib_cc_library(
    name='ITablet',
    srcs=[],
    deps=[
        ':CommitOptions', ':ITabletReader', ':ImportOptions', ':IndexRoot',
        ':OpenOptions', ':TabletId', ':VersionCoord', ':VersionMeta',
        '//aios/autil:NoCopyable'
    ]
)
indexlib_cc_library(
    name='Tablet',
    deps=[
        ':Fence', ':ITablet', ':ITabletFactory', ':ITabletMergeController',
        ':ITabletReader', ':IdGenerator', ':IndexRecoverStrategy', ':Locator',
        ':MemSegmentCreator', ':MetricsManager', ':OpenOptions', ':Segment',
        ':SegmentMeta', ':TabletCenter', ':TabletCommitter', ':TabletDumper',
        ':TabletInfos', ':TabletLoader', ':TabletMemoryCalculator',
        ':TabletMetrics', ':TabletReader', ':TabletReaderContainer',
        ':TabletResource', ':TabletSchemaManager', ':TabletWriter', ':TaskType',
        ':Version', ':VersionCommitter', ':VersionLoader', ':VersionMerger',
        ':VersionMetaCreator', '//aios/future_lite',
        '//aios/storage/indexlib/base:MemoryQuotaController',
        '//aios/storage/indexlib/base:MemoryQuotaSynchronizer',
        '//aios/storage/indexlib/base:NoExceptionWrapper',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/config:BackgroundTaskConfig',
        '//aios/storage/indexlib/config:TabletOptions',
        '//aios/storage/indexlib/config:schema',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework/cleaner:OnDiskIndexCleaner',
        '//aios/storage/indexlib/framework/cleaner:ResourceCleaner',
        '//aios/storage/indexlib/framework/lifecycle',
        '//aios/storage/indexlib/framework/mem_reclaimer:EpochBasedMemReclaimer'
    ]
)
indexlib_cc_library(
    name='TabletFactoryCreator',
    hdrs=['ITabletFactory.h', 'TabletFactoryCreator.h'],
    deps=[
        '//aios/autil:lock', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/util:Singleton'
    ]
)
indexlib_cc_library(
    name='ITabletFactory',
    srcs=[],
    deps=[
        ':SegmentMeta', ':TabletFactoryCreator', ':TabletResource',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types'
    ]
)
indexlib_cc_library(
    name='TabletWriter',
    srcs=[],
    deps=[
        ':OpenOptions', ':SegmentDumper', ':Version', '//aios/autil:NoCopyable',
        '//aios/future_lite', '//aios/storage/indexlib/base:Status'
    ]
)
indexlib_cc_library(
    name='TabletReader',
    deps=[
        ':ITabletReader', ':ReadResource', ':TabletData',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/framework/mem_reclaimer:IIndexMemoryReclaimer'
    ]
)
indexlib_cc_library(
    name='VersionCommitter',
    deps=[
        ':Fence', ':Version', '//aios/storage/indexlib/base:NoExceptionWrapper',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/config:schema',
        '//aios/storage/indexlib/file_system'
    ]
)
indexlib_cc_library(
    name='VersionLoader',
    deps=[
        ':Version', '//aios/storage/indexlib/base:NoExceptionWrapper',
        '//aios/storage/indexlib/base:PathUtil',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/file_system'
    ]
)
indexlib_cc_library(
    name='TabletSchemaLoader',
    deps=[
        ':TabletFactoryCreator', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/config:SchemaResolver',
        '//aios/storage/indexlib/config:TabletSchemaDelegation',
        '//aios/storage/indexlib/config:options',
        '//aios/storage/indexlib/config:schema',
        '//aios/storage/indexlib/file_system'
    ]
)
indexlib_cc_library(
    name='VersionMerger',
    deps=[
        ':ITabletMergeController', ':MetricsManager', ':VersionCoord',
        ':VersionLoader', '//aios/alog', '//aios/autil:scope',
        '//aios/future_lite', '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/framework/index_task:IndexTaskMetrics',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResourceManager'
    ]
)
indexlib_cc_library(name='TaskType')
indexlib_cc_library(
    name='TabletMemoryCalculator',
    deps=[
        ':TabletData', ':TabletReaderContainer', ':TabletWriter',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/file_system'
    ]
)
indexlib_cc_library(
    name='MetricsWrapper',
    srcs=[],
    deps=[
        '//aios/kmonitor:kmonitor_client_cpp',
        '//aios/storage/indexlib/util/metrics:metric'
    ]
)
indexlib_cc_library(
    name='IndexRecoverStrategy',
    deps=[
        ':Version', ':VersionLoader',
        '//aios/storage/indexlib/base:NoExceptionWrapper',
        '//aios/storage/indexlib/base:PathUtil',
        '//aios/storage/indexlib/file_system'
    ]
)
indexlib_cc_library(
    name='TabletMetrics',
    deps=[
        ':BuildDocumentMetrics', ':IMetrics', ':MetricsWrapper',
        ':TabletDumper', ':TabletFault', ':TabletInfos',
        ':TabletMemoryCalculator', ':TabletReaderContainer', ':TabletWriter',
        '//aios/autil:time', '//aios/kmonitor:kmonitor_client_cpp',
        '//aios/storage/indexlib/base:MemoryQuotaController',
        '//aios/storage/indexlib/config:TabletOptions'
    ]
)
indexlib_cc_library(
    name='BuildDocumentMetrics',
    deps=[
        ':MetricsWrapper', '//aios/autil:time',
        '//aios/kmonitor:kmonitor_client_cpp',
        '//aios/storage/indexlib/document:DocumentBatch'
    ]
)
indexlib_cc_library(
    name='Fence',
    deps=[
        ':TabletId', '//aios/autil:string_helper',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/util:Base62',
        '//aios/storage/indexlib/util:epochid_util',
        '//aios/storage/indexlib/util:ip_convertor',
        '//aios/storage/indexlib/util:retry_util'
    ]
)
indexlib_cc_library(name='IdGenerator', deps=[':Segment', ':Version'])
indexlib_cc_library(
    name='VersionCoord',
    deps=[
        '//aios/autil:json', '//aios/autil:string_helper',
        '//aios/storage/indexlib/base:constants'
    ]
)
indexlib_cc_library(
    name='VersionMeta',
    deps=[
        ':Version', ':VersionLine', '//aios/autil:json',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants'
    ]
)
indexlib_cc_library(
    name='VersionMetaCreator',
    deps=[
        'VersionMeta', ':DeployIndexUtil', ':TabletData',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants'
    ]
)
indexlib_cc_library(
    name='DeployIndexUtil',
    deps=[
        ':Segment', ':Version', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system/load_config:interface'
    ]
)
indexlib_cc_library(
    name='ITabletMergeController',
    srcs=[],
    deps=[
        '//aios/autil:NoCopyable', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/framework/index_task:IIndexTaskPlanCreator',
        '//aios/storage/indexlib/framework/index_task:MergeTaskDefine'
    ]
)
indexlib_cc_library(
    name='TabletCreator',
    deps=[
        ':ITablet', ':Tablet', ':TabletResource', '//aios/autil:NoCopyable',
        '//aios/autil:log'
    ]
)
indexlib_cc_library(name='IMetrics', srcs=[])
indexlib_cc_library(
    name='MetricsManager',
    deps=[
        ':IMetrics', '//aios/autil:NoCopyable',
        '//aios/kmonitor:kmonitor_client_cpp',
        '//aios/storage/indexlib/util/counter'
    ]
)
indexlib_cc_library(name='IResource', srcs=[], deps=['//aios/autil:NoCopyable'])
indexlib_cc_library(
    name='ResourceMap',
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/framework:IResource'
    ]
)
indexlib_cc_library(
    name='TabletDataInfo',
    srcs=[],
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants'
    ]
)
indexlib_cc_library(
    name='TabletDataSchemaGroup',
    deps=[':IResource', '//aios/storage/indexlib/base:Types']
)
indexlib_cc_library(
    name='TabletDeployer',
    deps=[
        ':Version', ':VersionDeployDescription', ':VersionLoader',
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/config:options',
        '//aios/storage/indexlib/file_system:deploy',
        '//aios/storage/indexlib/file_system/load_config:interface',
        '//aios/storage/indexlib/framework/hooks',
        '//aios/storage/indexlib/framework/lifecycle'
    ]
)
indexlib_cc_library(
    name='SegmentFenceDirFinder',
    deps=[
        ':Fence', ':Version', '//aios/autil:json',
        '//aios/storage/indexlib/base:PathUtil',
        '//aios/storage/indexlib/file_system'
    ]
)
indexlib_cc_library(
    name='SegmentInfo',
    deps=[
        ':Locator', ':SegmentStatistics', '//aios/autil:json',
        '//aios/storage/indexlib/file_system'
    ]
)
indexlib_cc_library(
    name='VersionDeployDescription',
    deps=[
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/file_system:deploy',
        '//aios/storage/indexlib/file_system:interface'
    ]
)
indexlib_cc_library(
    name='SegmentStatistics',
    deps=[
        '//aios/autil:json', '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants'
    ]
)
indexlib_cc_library(
    name='VersionImporter',
    deps=[
        ':Fence', ':ImportOptions', ':SegmentInfo', ':Version',
        '//aios/storage/indexlib/base:PathUtil',
        '//aios/storage/indexlib/file_system'
    ]
)
indexlib_cc_library(
    name='VersionValidator',
    deps=[
        ':ITablet', ':ITabletFactory', ':ITabletValidator', ':MetricsManager',
        ':TabletSchemaLoader', '//aios/storage/indexlib/config:TabletOptions',
        '//aios/storage/indexlib/config:TabletSchema'
    ]
)
indexlib_cc_library(
    name='VersionCleaner',
    deps=[
        ':Segment', ':SegmentFenceDirFinder', ':Version', ':VersionCoord',
        ':VersionLoader', '//aios/filesystem/fslib:fslib-framework'
    ]
)
indexlib_cc_library(
    name='MemSegmentCreator',
    deps=[
        ':ITabletFactory', ':IdGenerator', ':MemSegment', ':TabletData',
        '//aios/autil:log', '//aios/storage/indexlib/config:options'
    ]
)
indexlib_cc_library(name='SegmentGroupMetrics', srcs=[])
indexlib_cc_library(
    name='SegmentMetrics',
    deps=[
        ':SegmentGroupMetrics', '//aios/autil:json',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/util:Exception'
    ]
)
indexlib_cc_library(
    name='VersionLine',
    deps=[
        ':VersionCoord', '//aios/autil:json',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants'
    ]
)
indexlib_cc_library(
    name='TabletFault',
    deps=[
        '//aios/autil:NoCopyable', '//aios/autil:json', '//aios/autil:log',
        '//aios/autil:time'
    ]
)
