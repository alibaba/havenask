load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
indexlib_cc_library(
    name='DeleteValueEncoder',
    deps=[
        '//aios/storage/indexlib/document:RawDocument',
        '//aios/storage/indexlib/index/common/field_format/attribute:attribute_convertor',
        '//aios/storage/indexlib/index/kv/config:ValueConfig'
    ]
)
indexlib_cc_library(
    name='AggregationDocumentFactory',
    deps=[
        '//aios/storage/indexlib/document:DocumentFactoryAdapter',
        '//aios/storage/indexlib/document:PlainDocumentParser'
    ]
)
