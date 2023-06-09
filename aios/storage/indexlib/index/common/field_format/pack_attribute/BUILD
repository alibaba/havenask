load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=[':__subpackages__'])
indexlib_cc_library(
    name='pack_attribute',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':AttributeReference', ':FloatCompressConvertor',
        ':PackAttributeFormatter', ':PackValueAdapter', ':PlainFormatEncoder'
    ]
)
indexlib_cc_library(
    name='AttributeReference',
    srcs=[],
    hdrs=['AttributeReference.h', 'AttributeReferenceTyped.h'],
    deps=[
        ':FloatCompressConvertor', '//aios/autil:const_string_util',
        '//aios/autil:pack_data', '//aios/autil:string_helper',
        '//aios/storage/indexlib/base:FieldTypeUtil',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/config:CompressTypeOption',
        '//aios/storage/indexlib/index/common/field_format/attribute:MultiValueAttributeFormatter'
    ]
)
indexlib_cc_library(
    name='PlainFormatEncoder',
    deps=[
        ':AttributeReference', '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/util:ByteSimilarityHasher'
    ]
)
indexlib_cc_library(
    name='PackAttributeFormatter',
    deps=[
        ':AttributeReference', ':PlainFormatEncoder',
        '//aios/autil:const_string_util', '//aios/autil:mem_pool_base',
        '//aios/autil:multi_value',
        '//aios/storage/indexlib/index/common/field_format/attribute:DefaultAttributeValueInitializer',
        '//aios/storage/indexlib/util:MemBuffer',
        '//aios/storage/indexlib/util/slice_array'
    ]
)
indexlib_cc_library(
    name='FloatCompressConvertor',
    srcs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/autil:mem_pool_base', '//aios/autil:multi_value',
        '//aios/autil:string_helper', '//aios/storage/indexlib/base:Define',
        '//aios/storage/indexlib/index/common/field_format/attribute:MultiValueAttributeFormatter'
    ]
)
indexlib_cc_library(
    name='PackValueAdapter',
    deps=[
        ':PackAttributeFormatter',
        '//aios/storage/indexlib/index/common/field_format/attribute:attribute_convertor'
    ]
)
indexlib_cc_library(
    name='PackValueComparator',
    deps=[
        ':PackAttributeFormatter',
        '//aios/storage/indexlib/config:SortDescription',
        '//aios/storage/indexlib/index/kv/config:ValueConfig'
    ],
    visibility=['//aios/storage/indexlib:__subpackages__']
)
