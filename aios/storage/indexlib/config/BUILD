load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
indexlib_cc_library(
    name='config',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':options', ':schema']
)
indexlib_cc_library(
    name='interface',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':IIndexConfig', ':ITabletSchema']
)
indexlib_cc_library(
    name='options',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':TabletOptions']
)
indexlib_cc_library(
    name='schema',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':TabletSchema']
)
indexlib_cc_library(
    name='sort_description',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':SortDescription']
)
indexlib_cc_library(name='MutableJson', deps=['//aios/autil:json'])
indexlib_cc_library(
    name='BuildConfig', deps=['//aios/autil:env_util', '//aios/autil:json']
)
indexlib_cc_library(
    name='IndexTaskConfig',
    deps=[
        '//aios/autil:json', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/legacy/config:OfflineMergeConfig'
    ]
)
indexlib_cc_library(
    name='BackgroundTaskConfig',
    deps=['//aios/autil:env_util', '//aios/autil:json']
)
indexlib_cc_library(
    name='MergeStrategyParameter',
    deps=['//aios/autil:json', '//aios/autil:string_helper']
)
indexlib_cc_library(
    name='MergeConfig',
    deps=[
        ':MergeStrategyParameter', '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/index/inverted_index/config:TruncateOptionConfig',
        '//aios/storage/indexlib/index/inverted_index/config:TruncateStrategy'
    ]
)
indexlib_cc_library(
    name='OnlineConfig',
    deps=[
        ':BuildConfig', '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/file_system:interface'
    ]
)
indexlib_cc_library(
    name='OfflineConfig',
    deps=[
        ':BuildConfig', ':IndexTaskConfig', ':MergeConfig', '//aios/autil:json',
        '//aios/storage/indexlib/file_system/load_config:interface'
    ]
)
indexlib_cc_library(
    name='TabletOptions',
    deps=[
        ':BackgroundTaskConfig', ':BuildOptionConfig', ':MutableJson',
        ':OfflineConfig', ':OnlineConfig', '//aios/autil:json',
        '//aios/autil:log',
        '//aios/storage/indexlib/legacy/config:OfflineMergeConfig'
    ]
)
indexlib_cc_library(
    name='BuildOptionConfig',
    deps=[
        ':SortDescription', ':TabletSchema', '//aios/autil:json',
        '//aios/autil:log'
    ]
)
indexlib_cc_library(
    name='IIndexConfig',
    srcs=[],
    deps=[
        ':FieldConfig', '//aios/autil:json',
        '//aios/storage/indexlib/base:Types'
    ]
)
indexlib_cc_library(
    name='IndexConfigHash', deps=[':IIndexConfig', '//aios/autil:hash_base']
)
indexlib_cc_library(
    name='FieldConfig',
    deps=[
        ':ConfigDefine', '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/util:TimestampUtil'
    ]
)
indexlib_cc_library(name='ITabletSchema', srcs=[], deps=['//aios/autil:json'])
indexlib_cc_library(
    name='TabletSchema',
    deps=[
        ':ITabletSchema', ':UnresolvedSchema', '//aios/autil:json',
        '//aios/autil:log', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types'
    ]
)
indexlib_cc_library(
    name='UnresolvedSchema',
    srcs=['LegacySchemaConvertor.cpp', 'UnresolvedSchema.cpp'],
    hdrs=['LegacySchemaConvertor.h', 'UnresolvedSchema.h'],
    implementation_deps=['//aios/storage/indexlib/indexlib/config'],
    deps=[
        ':FieldConfig', ':IIndexConfig', ':IndexConfigDeserializeResource',
        ':MutableJson', '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index:IndexFactoryCreator',
        '//aios/storage/indexlib/index/common:constants',
        '//aios/storage/indexlib/index/inverted_index:config',
        '//aios/storage/indexlib/index/inverted_index:constants',
        '//aios/storage/indexlib/index/orc:constants'
    ]
)
indexlib_cc_library(
    name='SchemaResolver',
    deps=[
        ':TabletSchema', ':UnresolvedSchema',
        '//aios/storage/indexlib/base:Status'
    ]
)
indexlib_cc_library(name='SortDescription', deps=['//aios/autil:json'])
indexlib_cc_library(
    name='SortParam',
    deps=[
        ':SortDescription', '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/base:Constant',
        '//aios/storage/indexlib/util:Exception'
    ]
)
indexlib_cc_library(name='ConfigDefine', srcs=[])
indexlib_cc_library(
    name='CompressTypeOption',
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/util:BlockFpEncoder',
        '//aios/storage/indexlib/util:FloatInt8Encoder',
        '//aios/storage/indexlib/util:Fp16Encoder',
        '//aios/storage/indexlib/util:Half'
    ]
)
indexlib_cc_library(
    name='GroupDataParameter',
    deps=[
        ':CompressTypeOption', ':ConfigDefine', ':FileCompressConfig',
        '//aios/autil:log', '//aios/storage/indexlib/base:Status'
    ]
)
indexlib_cc_library(
    name='FileCompressConfig',
    srcs=['FileCompressConfig.cpp', 'FileCompressSchema.cpp'],
    hdrs=['FileCompressConfig.h', 'FileCompressSchema.h'],
    deps=[
        ':ConfigDefine', ':SingleFileCompressConfig', '//aios/autil:json',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/util:Exception',
        '//aios/storage/indexlib/util:KeyHasherTyped',
        '//aios/storage/indexlib/util:RegularExpression',
        '//aios/storage/indexlib/util:key_value_map',
        '//aios/storage/indexlib/util/buffer_compressor'
    ]
)
indexlib_cc_library(
    name='SingleFileCompressConfig',
    deps=[
        '//aios/autil:json', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/util:RegularExpression',
        '//aios/storage/indexlib/util/buffer_compressor'
    ]
)
indexlib_cc_library(
    name='FileCompressConfigV2', deps=[':SingleFileCompressConfig']
)
indexlib_cc_library(
    name='IndexConfigDeserializeResource',
    deps=[
        ':ConfigDefine', ':FieldConfig', ':FileCompressConfigV2',
        ':MutableJson', '//aios/autil:NoCopyable', '//aios/autil:json',
        '//aios/autil:log'
    ]
)
indexlib_cc_library(name='TabletSchemaDelegation', deps=[':SchemaResolver'])
