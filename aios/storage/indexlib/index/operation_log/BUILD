load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=[':__subpackages__'])
indexlib_cc_library(
    name='factory',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':OperationLogIndexFactory']
)
indexlib_cc_library(
    name='reader',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':DoNothingRedoStrategy', ':OperationLogIndexReader',
        ':TargetSegmentsRedoStrategy', ':operation_basic'
    ]
)
indexlib_cc_library(
    name='config',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':OperationFieldInfo', ':OperationLogConfig']
)
indexlib_cc_library(
    name='modifier',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':OperationLogProcessor']
)
indexlib_cc_library(
    name='constants',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':Common', ':Constant']
)
indexlib_cc_library(
    name='OperationLogIndexFactory',
    deps=[
        ':CompressOperationLogMemIndexer', ':OperationLogDiskIndexer',
        ':OperationLogIndexReader', ':OperationLogMemIndexer',
        ':OperationLogMetrics', '//aios/storage/indexlib/index:IIndexFactory'
    ],
    alwayslink=True
)
indexlib_cc_library(
    name='OperationLogIndexer',
    srcs=[],
    deps=[
        ':BatchOpLogIterator', ':operation_basic',
        '//aios/storage/indexlib/base:Types'
    ]
)
indexlib_cc_library(
    name='OperationLogMemIndexer',
    deps=[
        ':InMemSegmentOperationIterator', ':NormalSegmentOperationIterator',
        ':OperationBlock', ':OperationBlockInfo', ':OperationDumper',
        ':OperationFactory', ':OperationLogConfig', ':OperationLogIndexer',
        ':OperationLogMetrics', ':operation_basic',
        '//aios/storage/indexlib/document:DocumentBatch',
        '//aios/storage/indexlib/framework:SegmentDumpItem',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/util/memory_control'
    ]
)
indexlib_cc_library(
    name='OperationLogDiskIndexer',
    deps=[
        ':NormalSegmentOperationIterator', ':OperationBlock',
        ':OperationBlockInfo', ':OperationFactory', ':OperationLogConfig',
        ':OperationLogIndexer', ':operation_basic',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index:interface'
    ]
)
indexlib_cc_library(
    name='operation_basic',
    srcs=[],
    hdrs=[
        'OperationBase.h', 'OperationCreator.h', 'OperationCursor.h',
        'OperationMeta.h'
    ],
    deps=[
        ':Common', ':OperationBlock', ':OperationFieldInfo',
        '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/index/primary_key:util',
        '//aios/storage/indexlib/index/primary_key/config:PrimaryKeyIndexConfig'
    ]
)
indexlib_cc_library(
    name='OperationFactory',
    hdrs=['OperationFactory.h', 'SegmentOperationIterator.h'],
    deps=[
        ':OperationLogConfig', ':RemoveOperation', ':RemoveOperationCreator',
        ':UpdateFieldOperation', ':UpdateFieldOperationCreator',
        ':operation_basic', '//aios/autil:log', '//aios/autil:mem_pool_base'
    ]
)
indexlib_cc_library(
    name='OperationDumper',
    deps=[
        ':Constant', ':operation_basic', '//aios/autil:log',
        '//aios/autil:mem_pool_base', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system'
    ]
)
indexlib_cc_library(
    name='SegmentOperationIterator',
    hdrs=['OperationFactory.h', 'SegmentOperationIterator.h'],
    deps=[
        ':OperationBlock', ':OperationLogConfig', ':RemoveOperation',
        ':operation_basic', '//aios/autil:log',
        '//aios/storage/indexlib/framework:Locator'
    ]
)
indexlib_cc_library(
    name='InMemSegmentOperationIterator',
    hdrs=['InMemSegmentOperationIterator.h'],
    deps=[
        'SegmentOperationIterator', ':OperationBlock', ':OperationFactory',
        ':operation_basic', '//aios/autil:log'
    ]
)
indexlib_cc_library(
    name='OperationLogConfig',
    hdrs=['OperationLogConfig.h'],
    deps=[
        ':Common', '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/index/inverted_index/config:SingleFieldIndexConfig'
    ]
)
indexlib_cc_library(
    name='RemoveOperation',
    srcs=[],
    hdrs=['OperationBase.h', 'OperationRedoHint.h', 'RemoveOperation.h'],
    deps=[
        ':OperationLogProcessor', '//aios/autil:log',
        '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/index/primary_key:reader'
    ]
)
indexlib_cc_library(
    name='OperationBase',
    srcs=[],
    hdrs=['OperationBase.h'],
    deps=[':OperationFieldInfo']
)
indexlib_cc_library(
    name='RemoveOperationCreator',
    hdrs=['RemoveOperationCreator.h'],
    deps=[
        ':OperationLogConfig', ':RemoveOperation', ':operation_basic',
        '//aios/autil:log', '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/config:IIndexConfig'
    ]
)
indexlib_cc_library(
    name='UpdateFieldOperation',
    srcs=[],
    hdrs=['OperationBase.h', 'OperationRedoHint.h', 'UpdateFieldOperation.h'],
    deps=[
        ':OperationFieldInfo', ':OperationLogProcessor', '//aios/autil:log',
        '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/index/primary_key:reader'
    ]
)
indexlib_cc_library(
    name='UpdateFieldOperationCreator',
    hdrs=['UpdateFieldOperationCreator.h'],
    deps=[
        ':OperationLogConfig', ':UpdateFieldOperation', ':operation_basic',
        '//aios/autil:log', '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/index/attribute:modifier'
    ]
)
indexlib_cc_library(
    name='OperationBlock',
    hdrs=['OperationBase.h', 'OperationBlock.h'],
    deps=[
        ':OperationFieldInfo', '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/base:NoExceptionWrapper',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/util:mmap_vector'
    ]
)
indexlib_cc_library(name='Common', srcs=[])
indexlib_cc_library(name='Constant', srcs=[])
indexlib_cc_library(
    name='FileOperationBlock',
    deps=[
        ':OperationBlock', ':OperationFactory', ':operation_basic',
        '//aios/storage/indexlib/file_system'
    ]
)
indexlib_cc_library(
    name='OperationFieldInfo',
    deps=[':OperationLogConfig', '//aios/storage/indexlib/file_system']
)
indexlib_cc_library(
    name='OperationBlockInfo',
    hdrs=['OperationBlockInfo.h'],
    deps=[':OperationBlock']
)
indexlib_cc_library(
    name='OperationLogProcessor',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types'
    ]
)
indexlib_cc_library(
    name='OperationLogReplayer',
    deps=[
        ':OperationIterator', ':OperationLogDiskIndexer',
        ':OperationLogMemIndexer', ':OperationRedoStrategy'
    ]
)
indexlib_cc_library(
    name='OperationLogIndexReader',
    deps=[
        ':OperationLogReplayer', '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:TabletData'
    ]
)
indexlib_cc_library(
    name='OperationIterator',
    deps=[
        ':OperationLogDiskIndexer', ':OperationLogMemIndexer',
        ':operation_basic', '//aios/autil:log'
    ]
)
indexlib_cc_library(
    name='OperationRedoStrategy',
    srcs=[],
    deps=['//aios/storage/indexlib/base:Types']
)
indexlib_cc_library(
    name='TargetSegmentsRedoStrategy',
    deps=[
        ':OperationBase', ':OperationRedoStrategy', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/framework:TabletData'
    ]
)
indexlib_cc_library(
    name='DoNothingRedoStrategy',
    deps=[
        ':OperationBase', ':OperationRedoStrategy', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status'
    ]
)
indexlib_cc_library(
    name='NormalSegmentOperationIterator',
    deps=[
        ':FileOperationBlock', ':InMemSegmentOperationIterator',
        ':operation_basic', '//aios/autil:log'
    ]
)
indexlib_cc_library(
    name='CompressOperationBlock',
    deps=[':OperationFactory', ':operation_basic', '//aios/autil:log']
)
indexlib_cc_library(
    name='CompressOperationLogMemIndexer',
    deps=[
        ':CompressOperationBlock', ':OperationLogMemIndexer',
        ':operation_basic', '//aios/autil:log'
    ]
)
indexlib_cc_library(
    name='OperationLogMetrics',
    deps=[
        '//aios/autil:log', '//aios/kmonitor:kmonitor_client_cpp',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/framework:IMetrics',
        '//aios/storage/indexlib/framework:MetricsWrapper'
    ]
)
indexlib_cc_library(
    name='BatchOpLogIterator',
    deps=[
        ':FileOperationBlock', ':OperationFactory', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types'
    ]
)
indexlib_cc_library(
    name='SingleOperationLogBuilder',
    deps=[
        ':OperationLogMemIndexer',
        '//aios/storage/indexlib/index/common:IndexerOrganizerUtil'
    ]
)
indexlib_cc_library(
    name='OperationLogBuildWorkItem',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':SingleOperationLogBuilder',
        '//aios/storage/indexlib/index/common:BuildWorkItem'
    ]
)
