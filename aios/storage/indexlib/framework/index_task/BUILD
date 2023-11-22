load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='task_execute',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[
        ':IIndexTaskPlanCreator', ':IndexTaskContext',
        ':IndexTaskContextCreator', ':LocalExecuteEngine'
    ]
)
strict_cc_library(name='BasicDefs', srcs=[])
strict_cc_library(
    name='IndexOperationDescription',
    deps=[
        ':BasicDefs', '//aios/autil:env_util', '//aios/autil:json',
        '//aios/storage/indexlib/base:Constant',
        '//aios/storage/indexlib/util:Traits'
    ]
)
strict_cc_library(
    name='IndexTaskHistory',
    deps=[
        ':BasicDefs', '//aios/autil:json', '//aios/storage/indexlib/base:Types'
    ]
)
strict_cc_library(
    name='IndexTaskMetrics',
    deps=[
        ':BasicDefs', ':IndexTaskHistory', '//aios/autil:log',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/framework:IMetrics',
        '//aios/storage/indexlib/framework:MetricsWrapper',
        '//aios/storage/indexlib/util/metrics:metric_reporter'
    ]
)
strict_cc_library(
    name='IndexOperation',
    deps=[
        ':IndexOperationMetrics', ':IndexTaskContext',
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/framework:MetricsManager'
    ]
)
strict_cc_library(
    name='IndexOperationMetrics',
    deps=[
        '//aios/kmonitor:kmonitor_client_cpp',
        '//aios/storage/indexlib/framework:IMetrics',
        '//aios/storage/indexlib/framework:MetricsWrapper'
    ]
)
strict_cc_library(
    name='IIndexOperationCreator',
    srcs=[],
    deps=[
        ':IndexOperation', ':IndexOperationDescription',
        '//aios/storage/indexlib/base:Status'
    ]
)
strict_cc_library(
    name='CustomIndexTaskFactory',
    srcs=[],
    deps=[
        ':IIndexOperationCreator', ':IIndexTaskPlanCreator',
        '//aios/autil:NoCopyable'
    ]
)
strict_cc_library(
    name='CustomIndexTaskFactoryCreator',
    hdrs=['CustomIndexTaskFactory.h', 'CustomIndexTaskFactoryCreator.h'],
    deps=[
        ':IIndexOperationCreator', ':IIndexTaskPlanCreator',
        '//aios/autil:NoCopyable', '//aios/storage/indexlib/util:Singleton'
    ]
)
strict_cc_library(
    name='IndexTaskPlan',
    deps=[
        ':IndexOperationDescription', '//aios/autil:json', '//aios/autil:log'
    ]
)
strict_cc_library(
    name='IIndexTaskPlanCreator',
    srcs=[],
    deps=[
        ':IndexOperation', ':IndexTaskPlan',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:constants'
    ]
)
strict_cc_library(
    name='IndexTaskResource',
    srcs=[],
    deps=[
        ':BasicDefs', '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status'
    ]
)
strict_cc_library(name='ExtendResource', srcs=[], deps=[':IndexTaskResource'])
strict_cc_library(
    name='IIndexTaskResourceCreator',
    srcs=[],
    deps=[':BasicDefs', ':IndexTaskResource']
)
strict_cc_library(
    name='IndexTaskResourceManager',
    deps=[
        ':BasicDefs', ':ExtendResource', ':IIndexTaskResourceCreator',
        ':IndexTaskResource', '//aios/autil:NoCopyable',
        '//aios/storage/indexlib/base:NoExceptionWrapper',
        '//aios/storage/indexlib/file_system'
    ]
)
strict_cc_library(
    name='IndexTaskContext',
    deps=[
        ':BasicDefs', ':Constant',
        '//aios/storage/indexlib/base:NoExceptionWrapper',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system/relocatable:relocator',
        '//aios/storage/indexlib/framework:Fence',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/framework:tablet_schema_loader',
        '//aios/storage/indexlib/util:Clock'
    ]
)
strict_cc_library(
    name='MergeTaskDefine',
    srcs=[],
    deps=['//aios/storage/indexlib/base:constants']
)
strict_cc_library(
    name='IndexTaskContextCreator',
    deps=[
        ':BasicDefs', ':IIndexOperationCreator', ':IIndexTaskResourceCreator',
        ':IndexTaskContext', ':IndexTaskResourceManager',
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/base:MemoryQuotaController',
        '//aios/storage/indexlib/base:NoExceptionWrapper',
        '//aios/storage/indexlib/config:TabletOptions',
        '//aios/storage/indexlib/framework:DiskSegment',
        '//aios/storage/indexlib/framework:EnvironmentVariablesProvider',
        '//aios/storage/indexlib/framework:ITabletFactory',
        '//aios/storage/indexlib/framework:MetricsManager',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/framework:VersionLoader',
        '//aios/storage/indexlib/framework:tablet_schema_loader'
    ]
)
strict_cc_library(
    name='LocalExecuteEngine',
    deps=[
        ':IIndexOperationCreator', ':IndexTaskContext', ':IndexTaskPlan',
        '//aios/autil:NoCopyable', '//aios/autil:log', '//aios/future_lite',
        '//aios/storage/indexlib/base:Status'
    ]
)
strict_cc_library(name='Constant', srcs=[])
