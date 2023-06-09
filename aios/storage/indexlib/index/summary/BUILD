load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=[':__subpackages__'])
indexlib_cc_library(
    name='factory',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':SummaryIndexFactory']
)
indexlib_cc_library(
    name='reader',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':SummaryReader']
)
indexlib_cc_library(
    name='config',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=['//aios/storage/indexlib/index/summary/config:SummaryIndexConfig']
)
indexlib_cc_library(
    name='constants',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':Common', ':Constant', ':Types']
)
indexlib_cc_library(name='Common', srcs=[])
indexlib_cc_library(
    name='Constant',
    srcs=[],
    deps=[
        ':Types', '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/index/common:Types'
    ]
)
indexlib_cc_library(name='Types', srcs=[])
indexlib_cc_library(
    name='SummaryIndexFactory',
    deps=[
        ':Common', ':Constant', ':SummaryDiskIndexer', ':SummaryMemIndexer',
        ':SummaryMerger', ':SummaryReader', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/index:IIndexFactory'
    ],
    alwayslink=True
)
indexlib_cc_library(
    name='SummaryMemIndexer',
    deps=[
        ':Common', ':Constant', ':LocalDiskSummaryMemIndexer',
        ':SummaryMemReaderContainer', '//aios/autil:log',
        '//aios/storage/indexlib/base:PathUtil',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/document/extractor:IDocumentInfoExtractorFactory',
        '//aios/storage/indexlib/document/extractor/plain:SummaryDocInfoExtractor',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/summary/config:SummaryIndexConfig'
    ]
)
indexlib_cc_library(
    name='LocalDiskSummaryMemIndexer',
    deps=[
        ':Common', ':Constant', ':SummaryMemReader', '//aios/autil:log',
        '//aios/storage/indexlib/base:PathUtil',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework:DumpParams',
        '//aios/storage/indexlib/index/common:GroupFieldDataWriter'
    ]
)
indexlib_cc_library(
    name='SummaryMemReader',
    deps=[
        ':Common', ':Constant', '//aios/autil:log',
        '//aios/storage/indexlib/base:PathUtil',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/document/normal:SummaryGroupFormatter',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataAccessor'
    ]
)
indexlib_cc_library(
    name='SummaryMemReaderContainer',
    deps=[
        ':Common', ':Constant', ':SummaryMemReader', '//aios/autil:log',
        '//aios/storage/indexlib/base:PathUtil',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/common:Types'
    ]
)
indexlib_cc_library(
    name='SummaryReader',
    deps=[
        ':Common', ':Constant', ':SummaryDiskIndexer', ':SummaryMemIndexer',
        ':SummaryMemReaderContainer', '//aios/autil:log',
        '//aios/storage/indexlib/base:PathUtil',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/attribute:reader',
        '//aios/storage/indexlib/index/primary_key:reader'
    ]
)
indexlib_cc_library(
    name='SummaryDiskIndexer',
    deps=[
        ':Common', ':Constant', ':LocalDiskSummaryDiskIndexer',
        '//aios/autil:log', '//aios/storage/indexlib/base:PathUtil',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/summary/config:SummaryIndexConfig'
    ]
)
indexlib_cc_library(
    name='LocalDiskSummaryDiskIndexer',
    deps=[
        ':Common', ':Constant', '//aios/autil:log',
        '//aios/storage/indexlib/base:PathUtil',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/document/normal:SearchSummaryDocument',
        '//aios/storage/indexlib/document/normal:SummaryGroupFormatter',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index:IndexerParameter',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataParamHelper',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataReader',
        '//aios/storage/indexlib/index/summary/config:SummaryIndexConfig'
    ]
)
indexlib_cc_library(
    name='SummaryMerger',
    deps=[
        ':LocalDiskSummaryMerger',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResourceManager',
        '//aios/storage/indexlib/index/summary/config:SummaryIndexConfig'
    ]
)
indexlib_cc_library(
    name='LocalDiskSummaryMerger',
    deps=[
        ':Common', ':Constant', ':SummaryDiskIndexer', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index:DocMapper',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/index/common:FileCompressParamHelper',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataMerger',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataParamHelper',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataReader',
        '//aios/storage/indexlib/index/common/data_structure:VarLenDataWriter'
    ]
)
indexlib_cc_library(
    name='SingleSummaryBuilder',
    deps=[
        ':SummaryMemIndexer',
        '//aios/storage/indexlib/document/normal:NormalDocument',
        '//aios/storage/indexlib/framework:TabletData'
    ]
)
indexlib_cc_library(
    name='SummaryBuildWorkItem',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':SingleSummaryBuilder',
        '//aios/storage/indexlib/index/common:BuildWorkItem'
    ]
)
