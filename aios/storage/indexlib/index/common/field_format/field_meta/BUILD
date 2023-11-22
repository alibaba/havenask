load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=[':__subpackages__'])
strict_cc_library(
    name='field_meta_convertor',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':FieldMetaConvertor']
)
strict_cc_library(
    name='FieldMetaConvertor',
    deps=[
        '//aios/storage/indexlib/index/common/field_format/attribute:attribute_convertor',
        '//aios/storage/indexlib/index/field_meta/config:FieldMetaConfig'
    ]
)
