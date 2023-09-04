load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='kkv_document',
    srcs=[],
    hdrs=['KKVDocumentBatch.h'],
    deps=[':KKVDocument', '//aios/storage/indexlib/document/kv:kv_document']
)
strict_cc_library(
    name='KKVDocument',
    srcs=[],
    deps=['//aios/storage/indexlib/document/kv:KVDocument']
)
strict_cc_library(
    name='KKVKeysExtractor',
    deps=[
        ':KKVDocument', '//aios/autil:log', '//aios/autil:string_type',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/index/common:KeyHasherWrapper',
        '//aios/storage/indexlib/index/kkv/config:KKVIndexConfig'
    ]
)
strict_cc_library(
    name='KKVIndexDocumentParser',
    deps=[
        ':KKVDocument', ':KKVKeysExtractor',
        '//aios/storage/indexlib/document/kv:KVIndexDocumentParserBase',
        '//aios/storage/indexlib/index/kkv/config:KKVIndexConfig'
    ]
)
strict_cc_library(
    name='KKVDocumentParser',
    deps=[
        ':KKVIndexDocumentParser',
        '//aios/storage/indexlib/document/kv:KVDocumentParser'
    ]
)
strict_cc_library(
    name='KKVDocumentFactory',
    deps=[
        ':KKVDocumentParser',
        '//aios/storage/indexlib/document:DocumentFactoryAdapter'
    ]
)
