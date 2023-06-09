load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
indexlib_cc_library(
    name='VirtualAttributeIndexFactory',
    deps=[
        ':Common', ':VirtualAttributeConfig', ':VirtualAttributeDiskIndexer',
        ':VirtualAttributeIndexReader', ':VirtualAttributeMemIndexer',
        '//aios/storage/indexlib/index:IIndexFactory',
        '//aios/storage/indexlib/index:IndexFactoryCreator'
    ],
    alwayslink=True
)
indexlib_cc_library(
    name='VirtualAttributeConfig',
    deps=[
        ':Common', '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/index/attribute:config'
    ]
)
indexlib_cc_library(
    name='VirtualAttributeIndexReader',
    srcs=[],
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/index:IIndexReader',
        '//aios/storage/indexlib/index/attribute:reader'
    ]
)
indexlib_cc_library(
    name='VirtualAttributeMemIndexer',
    deps=[
        ':VirtualAttributeConfig', '//aios/autil:log',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/attribute:indexer'
    ]
)
indexlib_cc_library(
    name='VirtualAttributeDiskIndexer',
    deps=[
        ':VirtualAttributeConfig', '//aios/autil:log',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/attribute:indexer'
    ]
)
indexlib_cc_library(
    name='SingleVirtualAttributeBuilder',
    deps=[
        ':VirtualAttributeConfig', ':VirtualAttributeDiskIndexer',
        ':VirtualAttributeMemIndexer', '//aios/autil:log',
        '//aios/storage/indexlib/index/attribute:AttributeBuildWorkItem'
    ]
)
indexlib_cc_library(
    name='VirtualAttributeBuildWorkItem',
    deps=[
        ':SingleVirtualAttributeBuilder',
        '//aios/storage/indexlib/index/attribute:AttributeBuildWorkItem'
    ]
)
indexlib_cc_library(name='Common', srcs=[])
