load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='VirtualAttributeIndexFactory',
    deps=[
        ':Common', ':VirtualAttributeConfig', ':VirtualAttributeDiskIndexer',
        ':VirtualAttributeIndexReader', ':VirtualAttributeMemIndexer',
        '//aios/storage/indexlib/index:IIndexFactory',
        '//aios/storage/indexlib/index:IndexFactoryCreator'
    ],
    alwayslink=True
)
strict_cc_library(
    name='VirtualAttributeConfig',
    deps=[
        ':Common', '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/index/attribute:config'
    ]
)
strict_cc_library(
    name='VirtualAttributeIndexReader',
    srcs=[],
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/index:IIndexReader',
        '//aios/storage/indexlib/index/attribute:reader'
    ]
)
strict_cc_library(
    name='VirtualAttributeMemIndexer',
    deps=[
        ':VirtualAttributeConfig', '//aios/autil:log',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/attribute:indexer'
    ]
)
strict_cc_library(
    name='VirtualAttributeDiskIndexer',
    deps=[
        ':VirtualAttributeConfig', '//aios/autil:log',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/attribute:indexer'
    ]
)
strict_cc_library(
    name='SingleVirtualAttributeBuilder',
    deps=[
        ':VirtualAttributeConfig', ':VirtualAttributeDiskIndexer',
        ':VirtualAttributeMemIndexer', '//aios/autil:log',
        '//aios/storage/indexlib/index/attribute:AttributeBuildWorkItem'
    ]
)
strict_cc_library(
    name='VirtualAttributeBuildWorkItem',
    deps=[
        ':SingleVirtualAttributeBuilder',
        '//aios/storage/indexlib/index/attribute:AttributeBuildWorkItem'
    ]
)
strict_cc_library(name='Common', srcs=[])
