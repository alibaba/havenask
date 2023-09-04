load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=[':__subpackages__'])
strict_cc_library(
    name='factory',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':DeletionMapIndexFactory']
)
strict_cc_library(
    name='reader',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':DeletionMapIndexReader']
)
strict_cc_library(
    name='modifier',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':DeletionMapModifier', ':DeletionMapPatchFileFinder',
        ':DeletionMapPatchWriter', ':DeletionMapPatcher'
    ]
)
strict_cc_library(
    name='merger',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':DeletionMapMerger']
)
strict_cc_library(
    name='config',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':DeletionMapConfig']
)
strict_cc_library(
    name='constants',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':Common']
)
strict_cc_library(
    name='DeletionMapIndexFactory',
    deps=[
        ':DeletionMapDiskIndexer', ':DeletionMapIndexReader',
        ':DeletionMapMemIndexer', ':DeletionMapMerger', ':DeletionMapMetrics',
        '//aios/autil:time', '//aios/storage/indexlib/index:IIndexFactory'
    ],
    alwayslink=True
)
strict_cc_library(
    name='DeletionMapMemIndexer',
    deps=[
        ':DeletionMapConfig', ':DeletionMapMetrics', ':DeletionMapUtil',
        '//aios/storage/indexlib/document:DocumentBatch',
        '//aios/storage/indexlib/document/normal:NormalDocument',
        '//aios/storage/indexlib/index:BuildingIndexMemoryUseUpdater',
        '//aios/storage/indexlib/index:DocMapDumpParams',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/util:ExpandableBitmap'
    ]
)
strict_cc_library(
    name='DeletionMapDiskIndexer',
    deps=[
        ':DeletionMapConfig', ':DeletionMapMetrics', ':DeletionMapUtil',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system:interface',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/util:Bitmap'
    ]
)
strict_cc_library(
    name='DeletionMapIndexReader',
    deps=[
        ':DeletionMapDiskIndexer', ':DeletionMapMemIndexer',
        ':DeletionMapMetrics', ':DeletionMapResource',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/index:interface'
    ]
)
strict_cc_library(
    name='DeletionMapMerger',
    deps=[
        ':DeletionMapDiskIndexer', '//aios/autil:log',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/index:IIndexMerger'
    ]
)
strict_cc_library(
    name='DeletionMapModifier',
    deps=[':DeletionMapIndexerOrganizerUtil', ':DeletionMapResource']
)
strict_cc_library(
    name='DeletionMapPatcher',
    deps=[
        ':DeletionMapDiskIndexer', ':DeletionMapMemIndexer',
        ':DeletionMapModifier', '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:TabletData'
    ]
)
strict_cc_library(
    name='Common',
    srcs=[],
    deps=['//aios/autil:string_helper', '//aios/storage/indexlib/base:Types']
)
strict_cc_library(
    name='DeletionMapConfig',
    deps=[':Common', '//aios/storage/indexlib/config:IIndexConfig']
)
strict_cc_library(
    name='DeletionMapMetrics',
    deps=[
        '//aios/storage/indexlib/framework:IMetrics',
        '//aios/storage/indexlib/framework:MetricsWrapper'
    ]
)
strict_cc_library(
    name='DeletionMapResource',
    srcs=[],
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/framework:IResource'
    ]
)
strict_cc_library(
    name='DeletionMapUtil',
    deps=[':Common', '//aios/autil:log', '//aios/storage/indexlib/file_system']
)
strict_cc_library(
    name='DeletionMapPatchWriter',
    deps=[
        ':DeletionMapUtil', '//aios/autil:log',
        '//aios/storage/indexlib/base:PathUtil',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/common/patch:PatchFileInfos',
        '//aios/storage/indexlib/index/common/patch:PatchWriter',
        '//aios/storage/indexlib/util:ExpandableBitmap'
    ]
)
strict_cc_library(
    name='DeletionMapPatchFileFinder',
    deps=[
        ':Common', ':DeletionMapUtil', '//aios/autil:log',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/common/patch:PatchFileFinder'
    ]
)
strict_cc_library(
    name='DeletionMapIndexerOrganizerUtil',
    deps=[
        ':DeletionMapConfig', ':DeletionMapDiskIndexer',
        ':DeletionMapMemIndexer', '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:TabletData'
    ]
)
strict_cc_library(
    name='SingleDeletionMapBuilder',
    deps=[
        'DeletionMapIndexerOrganizerUtil',
        '//aios/storage/indexlib/index/common:IndexerOrganizerUtil'
    ]
)
strict_cc_library(
    name='DeletionMapBuildWorkItem',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':SingleDeletionMapBuilder',
        '//aios/storage/indexlib/index/common:BuildWorkItem'
    ]
)
