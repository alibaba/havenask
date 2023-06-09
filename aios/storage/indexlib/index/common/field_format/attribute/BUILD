load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=[':__subpackages__'])
indexlib_cc_library(
    name='attribute_convertor',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':AttributeConvertorFactory']
)
indexlib_cc_library(
    name='type_info',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':TypeInfo']
)
indexlib_cc_library(
    name='decoder',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':CompactPackAttributeDecoder']
)
indexlib_cc_library(
    name='TypeInfo',
    srcs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/autil:NoCopyable', '//aios/autil:const_string_util',
        '//aios/autil:hash_base', '//aios/autil:long_hash_value',
        '//aios/autil:multi_value', '//aios/autil:string_helper',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/index/common:Types'
    ]
)
indexlib_cc_library(
    name='MultiValueAttributeFormatter',
    srcs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/autil:NoCopyable', '//aios/autil:multi_value',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants'
    ]
)
indexlib_cc_library(
    name='AttributeConvertorFactory',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':CompressFloatAttributeConvertor',
        ':CompressSingleFloatAttributeConvertor', ':DateAttributeConvertor',
        ':LineAttributeConvertor', ':LocationAttributeConvertor',
        ':MultiStringAttributeConvertor', ':MultiValueAttributeConvertor',
        ':PolygonAttributeConvertor', ':SingleValueAttributeConvertor',
        ':StringAttributeConvertor', ':TimeAttributeConvertor',
        ':TimestampAttributeConvertor', '//aios/autil:NoCopyable',
        '//aios/autil:multi_value', '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/index/attribute:config',
        '//aios/storage/indexlib/util:Singleton'
    ]
)
indexlib_cc_library(
    name='AttributeConvertor',
    srcs=[],
    deps=[
        '//aios/autil:NoCopyable', '//aios/autil:const_string_util',
        '//aios/autil:mem_pool_base', '//aios/storage/indexlib/base:Status'
    ]
)
indexlib_cc_library(
    name='SingleValueAttributeConvertor',
    srcs=[],
    deps=[
        ':AttributeConvertor', ':TypeInfo', '//aios/autil:NoCopyable',
        '//aios/autil:const_string_util',
        '//aios/storage/indexlib/util:ErrorLogCollector'
    ]
)
indexlib_cc_library(
    name='MultiValueAttributeConvertor',
    srcs=[],
    deps=[
        ':AttributeConvertor', ':MultiValueAttributeFormatter', ':TypeInfo',
        '//aios/storage/indexlib/util:ErrorLogCollector',
        '//aios/storage/indexlib/util:HashString'
    ]
)
indexlib_cc_library(
    name='TimestampAttributeConvertor',
    deps=[
        ':SingleValueAttributeConvertor', '//aios/autil:NoCopyable',
        '//aios/autil:const_string_util', '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/util:TimestampUtil'
    ]
)
indexlib_cc_library(
    name='TimeAttributeConvertor',
    deps=[
        ':SingleValueAttributeConvertor', '//aios/autil:NoCopyable',
        '//aios/autil:const_string_util', '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/util:TimestampUtil'
    ]
)
indexlib_cc_library(
    name='LocationAttributeConvertor',
    srcs=[],
    deps=[
        ':AttributeConvertor',
        '//aios/storage/indexlib/index/common/field_format/spatial:shape'
    ]
)
indexlib_cc_library(
    name='ShapeAttributeUtil',
    srcs=[],
    deps=[
        ':AttributeConvertor', '//aios/autil:NoCopyable',
        '//aios/autil:const_string_util', '//aios/autil:log',
        '//aios/storage/indexlib/index/common/field_format/spatial:shape'
    ]
)
indexlib_cc_library(
    name='LineAttributeConvertor',
    srcs=[],
    deps=[':LocationAttributeConvertor', ':ShapeAttributeUtil']
)
indexlib_cc_library(
    name='CompactPackAttributeDecoder',
    deps=[':AttributeConvertor', ':MultiValueAttributeFormatter']
)
indexlib_cc_library(
    name='StringAttributeConvertor',
    srcs=[],
    deps=[':AttributeConvertor', ':TypeInfo']
)
indexlib_cc_library(
    name='DateAttributeConvertor',
    deps=[
        ':SingleValueAttributeConvertor',
        '//aios/storage/indexlib/util:TimestampUtil'
    ]
)
indexlib_cc_library(
    name='PolygonAttributeConvertor',
    srcs=[],
    deps=[':LocationAttributeConvertor', ':ShapeAttributeUtil']
)
indexlib_cc_library(
    name='CompressSingleFloatAttributeConvertor',
    srcs=[],
    deps=[
        ':MultiValueAttributeFormatter', ':SingleValueAttributeConvertor',
        '//aios/autil:mem_pool_base'
    ]
)
indexlib_cc_library(
    name='CompressFloatAttributeConvertor',
    deps=[
        ':MultiValueAttributeConvertor', ':TypeInfo',
        '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/config:CompressTypeOption'
    ]
)
indexlib_cc_library(
    name='MultiStringAttributeConvertor',
    srcs=[],
    deps=[':MultiValueAttributeConvertor', '//aios/autil:data_buffer']
)
indexlib_cc_library(
    name='AttributeFieldPrinter',
    srcs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':ShapeAttributeUtil', '//aios/autil:data_buffer']
)
indexlib_cc_library(
    name='AttributeValueInitializer',
    srcs=[],
    hdrs=['AttributeValueInitializer.h', 'AttributeValueInitializerCreator.h'],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/autil:const_string_util', '//aios/autil:mem_pool_base',
        '//aios/storage/indexlib/base:Types'
    ]
)
indexlib_cc_library(
    name='DefaultAttributeValueInitializer',
    srcs=[
        'DefaultAttributeValueInitializer.cpp',
        'DefaultAttributeValueInitializerCreator.cpp'
    ],
    hdrs=[
        'DefaultAttributeValueInitializer.h',
        'DefaultAttributeValueInitializerCreator.h'
    ],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':AttributeConvertorFactory', ':AttributeValueInitializer']
)
