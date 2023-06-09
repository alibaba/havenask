load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
indexlib_cc_library(
    name='table',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':KVTabletFactory', ':reader']
)
indexlib_cc_library(
    name='reader',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':KVTabletSessionReader']
)
indexlib_cc_library(
    name='KVTabletFactory',
    visibility=['//aios/storage/indexlib/table:__subpackages__'],
    deps=[
        ':KVMemSegment', ':KVSchemaResolver', ':KVTabletExporter',
        ':KVTabletOptions', ':KVTabletSessionReader', ':KVTabletWriter',
        '//aios/autil:NoCopyable', '//aios/autil:env_util',
        '//aios/future_lite', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/document:DocumentBatch',
        '//aios/storage/indexlib/document/kv:KVDocumentFactory',
        '//aios/storage/indexlib/document/kv:KVDocumentParser',
        '//aios/storage/indexlib/framework:ITabletFactory',
        '//aios/storage/indexlib/framework:TabletLoader',
        '//aios/storage/indexlib/framework/index_task:IIndexOperationCreator',
        '//aios/storage/indexlib/framework/index_task:IIndexTaskPlanCreator',
        '//aios/storage/indexlib/framework/index_task:IIndexTaskResourceCreator',
        '//aios/storage/indexlib/index:IIndexFactory',
        '//aios/storage/indexlib/index/kv:factory',
        '//aios/storage/indexlib/table:builtin_define',
        '//aios/storage/indexlib/table/common:CommonTabletValidator',
        '//aios/storage/indexlib/table/common:LSMTabletLoader',
        '//aios/storage/indexlib/table/kv_table/index_task:KVTableResourceCreator',
        '//aios/storage/indexlib/table/kv_table/index_task:KVTableTaskOperationCreator',
        '//aios/storage/indexlib/table/kv_table/index_task:KVTableTaskPlanCreator',
        '//aios/storage/indexlib/table/plain:MultiShardDiskSegment',
        '//aios/storage/indexlib/table/plain:MultiShardMemSegment',
        '//aios/storage/indexlib/table/plain:PlainDiskSegment'
    ],
    alwayslink=True
)
indexlib_cc_library(
    name='KVTabletWriter',
    deps=[
        ':KVTabletOptions', ':KVTabletReader', '//aios/autil:result',
        '//aios/storage/indexlib/config:IndexConfigHash',
        '//aios/storage/indexlib/table/common:CommonTabletWriter'
    ]
)
indexlib_cc_library(
    name='KVTabletOptions',
    deps=['//aios/storage/indexlib/config:TabletOptions']
)
indexlib_cc_library(
    name='KVTabletSessionReader',
    deps=[
        ':KVTabletReader',
        '//aios/storage/indexlib/index/attribute:attr_helper',
        '//aios/storage/indexlib/table/common:CommonTabletSessionReader'
    ]
)
indexlib_cc_library(
    name='KVTabletReader',
    deps=[
        ':KVCacheReaderImpl', '//aios/storage/indexlib/framework:TabletReader',
        '//aios/storage/indexlib/index/attribute:attr_helper',
        '//aios/storage/indexlib/table/common:SearchUtil'
    ]
)
indexlib_cc_library(
    name='KVTabletExporter',
    deps=[
        ':KVTabletDocIterator',
        '//aios/storage/indexlib/framework:ITabletExporter'
    ]
)
indexlib_cc_library(
    name='KVTabletDocIterator',
    srcs=['KVTabletDocIterator.cpp', 'TabletDocIteratorBase.cpp'],
    hdrs=['KVTabletDocIterator.h', 'TabletDocIteratorBase.h'],
    deps=[
        '//aios/storage/indexlib/document:interface',
        '//aios/storage/indexlib/framework:ITabletDocIterator',
        '//aios/storage/indexlib/framework:Tablet',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/index/kv:factory',
        '//aios/storage/indexlib/table/plain:MultiShardDiskSegment'
    ]
)
indexlib_cc_library(
    name='KVSchemaResolver',
    implementation_deps=['//aios/storage/indexlib/indexlib/config'],
    deps=[
        '//aios/storage/indexlib/config:SchemaResolver',
        '//aios/storage/indexlib/config:UnresolvedSchema'
    ]
)
indexlib_cc_library(
    name='KVMemSegment',
    deps=[
        '//aios/storage/indexlib/index/kv:reader',
        '//aios/storage/indexlib/table/plain:MultiShardMemSegment'
    ]
)
indexlib_cc_library(
    name='KVReaderImpl',
    deps=[
        '//aios/storage/indexlib/index/kv:reader',
        '//aios/storage/indexlib/table/plain:MultiShardDiskSegment',
        '//aios/storage/indexlib/table/plain:MultiShardMemSegment'
    ]
)
indexlib_cc_library(name='KVCacheItem', srcs=[])
indexlib_cc_library(
    name='KVCacheReaderImpl', srcs=[], deps=[':KVCacheItem', ':KVReaderImpl']
)
