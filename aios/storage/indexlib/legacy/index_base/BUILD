load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
indexlib_cc_library(
    name='PartitionMeta',
    deps=[
        '//aios/autil:json', '//aios/storage/indexlib/config:SortDescription',
        '//aios/storage/indexlib/file_system'
    ]
)
indexlib_cc_library(
    name='SchemaAdapter',
    implementation_deps=['//aios/storage/indexlib/indexlib/config'],
    deps=['//aios/storage/indexlib/file_system']
)
