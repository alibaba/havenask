load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='PrimaryKeyLoadStrategyParam',
    deps=[
        '//aios/autil:log', '//aios/autil:string_helper',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/config:ConfigDefine'
    ]
)
strict_cc_library(
    name='PrimaryKeyIndexConfig',
    deps=[
        ':PrimaryKeyLoadStrategyParam',
        '//aios/storage/indexlib/config:IndexConfigDeserializeResource',
        '//aios/storage/indexlib/index/attribute/config',
        '//aios/storage/indexlib/index/inverted_index/config:SingleFieldIndexConfig',
        '//aios/storage/indexlib/index/primary_key:Types',
        '//aios/storage/indexlib/util:status_to_exception'
    ]
)
