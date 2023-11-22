load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='PartitionMeta',
    deps=[
        '//aios/autil:json', '//aios/storage/indexlib/config:SortDescription',
        '//aios/storage/indexlib/file_system'
    ]
)
strict_cc_library(
    name='SchemaAdapter',
    implementation_deps=['//aios/storage/indexlib/legacy/config:schema'],
    deps=['//aios/storage/indexlib/file_system']
)
