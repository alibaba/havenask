load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=[':__subpackages__'])
strict_cc_library(
    name='factory',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':SourceIndexFactory']
)
strict_cc_library(
    name='reader',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':SourceReader']
)
strict_cc_library(
    name='constants',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':Common', ':Constant', ':Types']
)
strict_cc_library(
    name='config',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/index/source/config:SourceGroupConfig',
        '//aios/storage/indexlib/index/source/config:SourceIndexConfig'
    ]
)
strict_cc_library(name='Common', srcs=[], deps=[':Constant'])
strict_cc_library(name='Types', srcs=[])
strict_cc_library(name='Constant', srcs=[])
strict_cc_library(
    name='SourceIndexFactory',
    deps=[
        ':Common', ':Constant', ':SourceDiskIndexer', ':SourceMemIndexer',
        ':SourceMerger', ':SourceReader', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/index:IIndexFactory',
        '//aios/storage/indexlib/index/source/config:SourceIndexConfig'
    ],
    alwayslink=True
)
strict_cc_library(
    name='SourceGroupWriter',
    deps=[
        ':Common', ':Constant',
        '//aios/storage/indexlib/index/common:GroupFieldDataWriter',
        '//aios/storage/indexlib/index/source/config:SourceGroupConfig'
    ]
)
strict_cc_library(
    name='SourceMemIndexer',
    deps=[
        ':Common', ':Constant', ':SourceGroupWriter',
        '//aios/storage/indexlib/document:DocumentBatch',
        '//aios/storage/indexlib/document/extractor:IDocumentInfoExtractorFactory',
        '//aios/storage/indexlib/document/normal:NormalDocument',
        '//aios/storage/indexlib/document/normal:SerializedSourceDocument',
        '//aios/storage/indexlib/document/normal:SourceDocument',
        '//aios/storage/indexlib/document/normal:SourceFormatter',
        '//aios/storage/indexlib/index:MemIndexerParameter',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/source/config:SourceIndexConfig'
    ]
)
strict_cc_library(
    name='SourceDiskIndexer',
    deps=[
        ':Common', ':Constant',
        '//aios/storage/indexlib/document/normal:SerializedSourceDocument',
        '//aios/storage/indexlib/document/normal:SourceDocument',
        '//aios/storage/indexlib/document/normal:SourceFormatter',
        '//aios/storage/indexlib/index:DiskIndexerParameter',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataParamHelper',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataReader',
        '//aios/storage/indexlib/index/source/config:SourceIndexConfig'
    ]
)
strict_cc_library(
    name='SourceReader',
    deps=[
        ':Common', ':Constant', ':SourceDiskIndexer', ':SourceMemIndexer',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/source/config:SourceIndexConfig'
    ]
)
strict_cc_library(
    name='SourceMerger',
    deps=[
        ':Common', ':Constant', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index:DocMapper',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/common:FileCompressParamHelper',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataMerger',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataParamHelper',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataReader',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataWriter',
        '//aios/storage/indexlib/index/source/config:SourceGroupConfig',
        '//aios/storage/indexlib/index/source/config:SourceIndexConfig',
        '//aios/storage/indexlib/util:SimplePool'
    ]
)
strict_cc_library(
    name='SingleSourceBuilder',
    deps=[
        ':SourceMemIndexer',
        '//aios/storage/indexlib/document/normal:NormalDocument',
        '//aios/storage/indexlib/framework:TabletData'
    ]
)
strict_cc_library(
    name='SourceBuildWorkItem',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':SingleSourceBuilder',
        '//aios/storage/indexlib/index/common:BuildWorkItem'
    ]
)
