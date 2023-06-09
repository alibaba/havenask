load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
indexlib_cc_library(
    name='config',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':KVIndexConfig']
)
indexlib_cc_library(
    name='KVIndexPreference',
    deps=[
        '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/index/kv:Constant',
        '//aios/storage/indexlib/index/kv:Types',
        '//aios/storage/indexlib/util:Exception',
        '//aios/storage/indexlib/util:key_value_map',
        '//aios/storage/indexlib/util/buffer_compressor'
    ]
)
indexlib_cc_library(
    name='ValueConfig',
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/index/attribute:config'
    ]
)
indexlib_cc_library(
    name='TTLSettings', deps=['//aios/storage/indexlib/base:Constant']
)
indexlib_cc_library(
    name='KVIndexConfig',
    deps=[
        ':KVIndexPreference', ':ValueConfig', ':TTLSettings',
        '//aios/storage/indexlib/index/kv:Common'
    ]
)
