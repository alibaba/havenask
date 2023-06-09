load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib/index:__subpackages__'])
indexlib_cc_library(
    name='config',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':AttributeConfig', ':PackAttributeConfig']
)
indexlib_cc_library(
    name='AttributeConfig',
    deps=[
        '//aios/storage/indexlib/config:CompressTypeOption',
        '//aios/storage/indexlib/config:ConfigDefine',
        '//aios/storage/indexlib/config:FileCompressConfig',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/config:IndexConfigDeserializeResource',
        '//aios/storage/indexlib/index/attribute:constants',
        '//aios/storage/indexlib/index/common:FieldTypeTraits',
        '//aios/storage/indexlib/index/common:constants',
        '//aios/storage/indexlib/index/inverted_index:constants'
    ]
)
indexlib_cc_library(name='PackAttributeConfig', deps=[':AttributeConfig'])
