load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(name='BlockHashDictionaryReader', srcs=[])
strict_cc_library(name='DictionaryTypedFactory', srcs=[])
strict_cc_library(
    name='LegacyBlockArrayReader',
    srcs=[],
    deps=[
        ':TieredDictionaryIterator',
        '//aios/storage/indexlib/index/common/block_array:reader'
    ]
)
strict_cc_library(
    name='IntegrateHashDictionaryReader',
    srcs=[],
    deps=[':DictionaryReader', ':HashDictionaryIterator']
)
strict_cc_library(name='UtilDefine', srcs=[])
strict_cc_library(
    name='DictionaryCreator',
    deps=[
        ':CommonDiskHashDictionaryReader', ':CommonDiskTieredDictionaryReader',
        ':DictionaryIterator', ':DictionaryReader', ':DictionaryTypedFactory',
        ':DictionaryWriter', ':HashDictionaryReader', ':HashDictionaryWriter',
        ':TieredDictionaryReader', ':TieredDictionaryWriter',
        '//aios/storage/indexlib/index/common:FileCompressParamHelper',
        '//aios/storage/indexlib/index/inverted_index/config:InvertedIndexConfig'
    ]
)
strict_cc_library(
    name='InMemDictionaryReader',
    deps=[
        ':DictionaryReader', '//aios/autil:snapshot_vector',
        '//aios/storage/indexlib/index/common:Constant',
        '//aios/storage/indexlib/util:HashMap'
    ]
)
strict_cc_library(
    name='TieredDictionaryReader',
    srcs=[],
    deps=[
        ':DictionaryReader', ':LegacyBlockArrayReader', ':UtilDefine',
        '//aios/autil:bloom_filter'
    ]
)
strict_cc_library(
    name='TieredDictionaryWriter',
    srcs=[],
    deps=[':DictionaryWriter', ':UtilDefine']
)
strict_cc_library(
    name='TieredDictionaryIterator', srcs=[], deps=[':DictionaryIterator']
)
strict_cc_library(
    name='CommonDiskHashDictionaryReader',
    srcs=[],
    deps=[':CommonDiskHashDictionaryIterator', ':DictionaryReader']
)
strict_cc_library(
    name='DefaultTermDictionaryReader',
    deps=[
        ':DictionaryReader',
        '//aios/storage/indexlib/index/inverted_index/format:DictInlineEncoder',
        '//aios/storage/indexlib/index/inverted_index/format:ShortListOptimizeUtil'
    ]
)
strict_cc_library(
    name='CommonDiskHashDictionaryIterator',
    srcs=[],
    deps=[':DictionaryIterator']
)
strict_cc_library(
    name='CommonDiskTieredDictionaryReader',
    srcs=[],
    deps=[
        ':CommonDiskTieredDictionaryIterator', ':DictionaryReader',
        '//aios/storage/indexlib/file_system/stream:file_stream'
    ]
)
strict_cc_library(
    name='CommonDiskTieredDictionaryIterator',
    srcs=[],
    deps=[':DictionaryIterator']
)
strict_cc_library(
    name='HashDictionaryReader',
    srcs=[],
    deps=[
        ':BlockHashDictionaryReader', ':DictionaryReader',
        ':IntegrateHashDictionaryReader'
    ]
)
strict_cc_library(
    name='HashDictionaryWriter',
    srcs=[],
    deps=[':DictionaryWriter', '//aios/storage/indexlib/util:simple_pool']
)
strict_cc_library(
    name='HashDictionaryIterator', srcs=[], deps=[':DictionaryIterator']
)
strict_cc_library(
    name='DictionaryReader',
    deps=[
        ':DictionaryIterator', ':UtilDefine', '//aios/future_lite',
        '//aios/storage/indexlib/index/common:DictKeyInfo',
        '//aios/storage/indexlib/index/common:error_code'
    ]
)
strict_cc_library(
    name='DictionaryWriter',
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/common:DictKeyInfo',
        '//aios/storage/indexlib/index/common:Types'
    ]
)
strict_cc_library(
    name='DictionaryIterator',
    srcs=[],
    deps=['//aios/autil:log', '//aios/storage/indexlib/index/common:Types']
)
