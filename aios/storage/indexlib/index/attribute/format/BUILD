load('//aios/storage:defs.bzl', 'strict_cc_library')
package(
    default_visibility=[
        '//aios/storage/indexlib/index/attribute:__subpackages__'
    ]
)
strict_cc_library(
    name='AttributeFormatter',
    deps=[
        '//aios/autil:NoCopyable', '//aios/autil:const_string_util',
        '//aios/autil:log', '//aios/autil:string_type',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format'
    ]
)
strict_cc_library(
    name='SingleEncodedNullValue', srcs=[], deps=['//aios/autil:NoCopyable']
)
strict_cc_library(
    name='SingleValueNullAttributeFormatter',
    srcs=[],
    deps=[
        ':SingleEncodedNullValue', '//aios/autil:NoCopyable',
        '//aios/autil:const_string_util', '//aios/autil:string_type',
        '//aios/future_lite', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format'
    ]
)
strict_cc_library(
    name='SingleValueAttributeMemFormatter',
    srcs=[],
    deps=[
        ':SingleEncodedNullValue', '//aios/autil:NoCopyable',
        '//aios/autil:const_string_util', '//aios/autil:log',
        '//aios/autil:string_type',
        '//aios/storage/indexlib/base:NoExceptionWrapper',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index:DocMapDumpParams',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/common:FileCompressParamHelper'
    ]
)
strict_cc_library(
    name='SingleValueAttributeFormatter',
    srcs=[],
    deps=[':AttributeFormatter', ':SingleValueNullAttributeFormatter']
)
strict_cc_library(
    name='SingleValueNullAttributeReadOnlyFormatter',
    srcs=[],
    visibility=['//visibility:private'],
    deps=[
        ':SingleValueNullAttributeFormatter',
        '//aios/storage/indexlib/file_system/stream:file_stream',
        '//aios/storage/indexlib/index/common:error_code',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format',
        '//aios/storage/indexlib/index/common/field_format/pack_attribute:FloatCompressConvertor'
    ]
)
strict_cc_library(
    name='SingleValueNullAttributeUpdatableFormatter',
    srcs=[],
    visibility=['//visibility:private'],
    deps=[
        ':SingleValueNullAttributeFormatter',
        '//aios/storage/indexlib/index/common:error_code',
        '//aios/storage/indexlib/index/common/field_format/pack_attribute:FloatCompressConvertor'
    ]
)
strict_cc_library(
    name='SingleValueAttributeReadOnlyFormatter',
    srcs=[],
    deps=[
        ':SingleValueAttributeFormatter',
        ':SingleValueNullAttributeReadOnlyFormatter'
    ]
)
strict_cc_library(
    name='SingleValueAttributeUpdatableFormatter',
    srcs=[],
    deps=[
        ':SingleValueAttributeFormatter',
        ':SingleValueNullAttributeUpdatableFormatter'
    ]
)
strict_cc_library(
    name='SingleValueAttributePatchFormatter',
    deps=[
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/file_system'
    ]
)
strict_cc_library(
    name='MultiValueAttributeDataFormatter',
    deps=[
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/storage/indexlib/file_system/stream:file_stream',
        '//aios/storage/indexlib/index/common:error_code',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format'
    ]
)
strict_cc_library(name='MultiValueAttributeOffsetUpdatableFormatter', srcs=[])
strict_cc_library(
    name='SingleValueDataAppender',
    deps=[
        ':SingleValueAttributeUpdatableFormatter', '//aios/autil:NoCopyable',
        '//aios/autil:log', '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system/file:interface',
        '//aios/storage/indexlib/index/common:data_structure',
        '//aios/storage/indexlib/index/common/field_format:attribute_field_format'
    ]
)
