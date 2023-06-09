load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
indexlib_cc_library(
    name='SummaryIndexConfig',
    deps=[
        ':SummaryConfig', ':SummaryGroupConfig', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/config:IndexConfigDeserializeResource',
        '//aios/storage/indexlib/config:SingleFileCompressConfig',
        '//aios/storage/indexlib/config:FileCompressConfigV2',
        '//aios/storage/indexlib/util:status_to_exception',
        '//aios/storage/indexlib/index/common:Types',
        '//aios/storage/indexlib/index/summary:Common',
        '//aios/storage/indexlib/index/summary:constants'
    ]
)
indexlib_cc_library(
    name='SummaryConfig',
    deps=[
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/config:FieldConfig'
    ]
)
indexlib_cc_library(
    name='SummaryGroupConfig',
    deps=[
        ':SummaryConfig', '//aios/storage/indexlib/config:GroupDataParameter',
        '//aios/storage/indexlib/index/summary:constants'
    ]
)
