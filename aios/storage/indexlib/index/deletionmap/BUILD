load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=[':__subpackages__'])
indexlib_cc_library(
    name='factory',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':DeletionMapIndexFactory']
)
indexlib_cc_library(
    name='reader',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':DeletionMapIndexReader']
)
indexlib_cc_library(
    name='modifier',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':DeletionMapModifier', ':DeletionMapPatchFileFinder',
        ':DeletionMapPatchWriter', ':DeletionMapPatcher'
    ]
)
indexlib_cc_library(
    name='merger',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':DeletionMapMerger']
)
indexlib_cc_library(
    name='config',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':DeletionMapConfig']
)
indexlib_cc_library(
    name='constants',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':Common']
)
indexlib_cc_library(
    name='DeletionMapIndexFactory',
    deps=[
        ':DeletionMapDiskIndexer', ':DeletionMapIndexReader',
        ':DeletionMapMemIndexer', ':DeletionMapMerger', ':DeletionMapMetrics',
        '//aios/autil:time', '//aios/storage/indexlib/index:IIndexFactory'
    ],
    alwayslink=True
)
indexlib_cc_library(
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
indexlib_cc_library(
    name='DeletionMapDiskIndexer',
    deps=[
        ':DeletionMapConfig', ':DeletionMapMetrics', ':DeletionMapUtil',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system:interface',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/util:Bitmap'
    ]
)
indexlib_cc_library(
    name='DeletionMapIndexReader',
    deps=[
        ':DeletionMapDiskIndexer', ':DeletionMapMemIndexer',
        ':DeletionMapMetrics', ':DeletionMapResource',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/index:interface'
    ]
)
indexlib_cc_library(
    name='DeletionMapMerger',
    deps=[
        ':DeletionMapDiskIndexer', '//aios/autil:log',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/index:IIndexMerger'
    ]
)
indexlib_cc_library(
    name='DeletionMapModifier',
    deps=[':DeletionMapIndexerOrganizerUtil', ':DeletionMapResource']
)
indexlib_cc_library(
    name='DeletionMapPatcher',
    deps=[
        ':DeletionMapDiskIndexer', ':DeletionMapMemIndexer',
        ':DeletionMapModifier', '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:TabletData'
    ]
)
indexlib_cc_library(
    name='Common',
    srcs=[],
    deps=['//aios/autil:string_helper', '//aios/storage/indexlib/base:Types']
)
indexlib_cc_library(
    name='DeletionMapConfig',
    deps=[':Common', '//aios/storage/indexlib/config:IIndexConfig']
)
indexlib_cc_library(
    name='DeletionMapMetrics',
    deps=[
        '//aios/storage/indexlib/framework:IMetrics',
        '//aios/storage/indexlib/framework:MetricsWrapper'
    ]
)
indexlib_cc_library(
    name='DeletionMapResource',
    srcs=[],
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/framework:IResource'
    ]
)
indexlib_cc_library(
    name='DeletionMapUtil',
    deps=[':Common', '//aios/autil:log', '//aios/storage/indexlib/file_system']
)
indexlib_cc_library(
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
indexlib_cc_library(
    name='DeletionMapPatchFileFinder',
    deps=[
        ':Common', ':DeletionMapUtil', '//aios/autil:log',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/common/patch:PatchFileFinder'
    ]
)
indexlib_cc_library(
    name='DeletionMapIndexerOrganizerUtil',
    deps=[
        ':DeletionMapConfig', ':DeletionMapDiskIndexer',
        ':DeletionMapMemIndexer', '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:TabletData'
    ]
)
indexlib_cc_library(
    name='SingleDeletionMapBuilder',
    deps=[
        'DeletionMapIndexerOrganizerUtil',
        '//aios/storage/indexlib/index/common:IndexerOrganizerUtil'
    ]
)
indexlib_cc_library(
    name='DeletionMapBuildWorkItem',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':SingleDeletionMapBuilder',
        '//aios/storage/indexlib/index/common:BuildWorkItem'
    ]
)
