package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
indexlib_cc_library(
    name='kv_document',
    srcs=['KVDocumentBatch.cpp'],
    hdrs=['KVDocumentBatch.h'],
    deps=[
        ':KVDocument', '//aios/autil:env_util',
        '//aios/storage/indexlib/document:DocumentBatch',
        '//aios/storage/indexlib/document:RawDocument',
        '//aios/storage/indexlib/util:Exception'
    ]
)
indexlib_cc_library(
    name='KVDocument',
    deps=[
        '//aios/autil:data_buffer',
        '//aios/storage/indexlib/document:interface',
        '//aios/storage/indexlib/index/kv:constants'
    ]
)
indexlib_cc_library(
    name='KVKeyExtractor',
    deps=[
        '//aios/autil:log', '//aios/autil:string_type',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/index/common:KeyHasherWrapper'
    ]
)
indexlib_cc_library(
    name='ValueConvertor',
    deps=[
        '//aios/autil:log', '//aios/autil:span',
        '//aios/storage/indexlib/config:FieldConfig',
        '//aios/storage/indexlib/document:RawDocument',
        '//aios/storage/indexlib/index/common/field_format/attribute:attribute_convertor',
        '//aios/storage/indexlib/index/common/field_format/pack_attribute',
        '//aios/storage/indexlib/index/kv/config:ValueConfig',
        '//aios/storage/indexlib/util:DocTracer',
        '//aios/storage/indexlib/util:ErrorLogCollector'
    ]
)
indexlib_cc_library(
    name='KVIndexDocumentParserBase',
    deps=[
        ':KVDocument', ':ValueConvertor',
        '//aios/storage/indexlib/index/kv/config:TTLSettings'
    ]
)
indexlib_cc_library(
    name='KVIndexDocumentParser',
    deps=[
        ':KVIndexDocumentParserBase', ':KVKeyExtractor',
        '//aios/storage/indexlib/config:IndexConfigHash',
        '//aios/storage/indexlib/index/kv/config:KVIndexConfig'
    ]
)
indexlib_cc_library(
    name='KVDocumentParser',
    deps=[
        ':KVIndexDocumentParser', ':kv_document',
        '//aios/storage/indexlib/config:TabletSchema',
        '//aios/storage/indexlib/document:BuiltinParserInitParam',
        '//aios/storage/indexlib/document:ExtendDocument'
    ]
)
indexlib_cc_library(
    name='KVDocumentFactory',
    deps=[
        ':KVDocumentParser',
        '//aios/storage/indexlib/document:DocumentFactoryAdapter'
    ]
)
