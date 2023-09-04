load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='config',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=['KKVIndexConfig']
)
strict_cc_library(
    name='KKVIndexConfig',
    deps=[
        ':KKVIndexFieldInfo', ':KKVIndexPreference', '//aios/autil:env_util',
        '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/config:IndexConfigDeserializeResource',
        '//aios/storage/indexlib/index/kkv:constants',
        '//aios/storage/indexlib/index/kv/config'
    ]
)
strict_cc_library(
    name='KKVIndexPreference',
    deps=[
        '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/index/kv/config:KVIndexPreference'
    ]
)
strict_cc_library(
    name='KKVIndexFieldInfo',
    deps=[
        '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/config:ConfigDefine',
        '//aios/storage/indexlib/config:SortParam'
    ]
)
