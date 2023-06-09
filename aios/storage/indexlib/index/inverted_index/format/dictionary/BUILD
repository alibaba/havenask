load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
indexlib_cc_library(name='BlockHashDictionaryReader', srcs=[])
indexlib_cc_library(name='DictionaryTypedFactory', srcs=[])
indexlib_cc_library(
    name='LegacyBlockArrayReader',
    srcs=[],
    deps=[
        ':TieredDictionaryIterator',
        '//aios/storage/indexlib/index/common/block_array:reader'
    ]
)
indexlib_cc_library(
    name='IntegrateHashDictionaryReader',
    srcs=[],
    deps=[':DictionaryReader', ':HashDictionaryIterator']
)
indexlib_cc_library(name='UtilDefine', srcs=[])
indexlib_cc_library(
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
indexlib_cc_library(
    name='InMemDictionaryReader',
    deps=[
        ':DictionaryReader', '//aios/autil:snapshot_vector',
        '//aios/storage/indexlib/index/common:Constant',
        '//aios/storage/indexlib/util:HashMap'
    ]
)
indexlib_cc_library(
    name='TieredDictionaryReader',
    srcs=[],
    deps=[
        ':DictionaryReader', ':LegacyBlockArrayReader', ':UtilDefine',
        '//aios/autil:bloom_filter'
    ]
)
indexlib_cc_library(
    name='TieredDictionaryWriter',
    srcs=[],
    deps=[':DictionaryWriter', ':UtilDefine']
)
indexlib_cc_library(
    name='TieredDictionaryIterator', srcs=[], deps=[':DictionaryIterator']
)
indexlib_cc_library(
    name='CommonDiskHashDictionaryReader',
    srcs=[],
    deps=[':CommonDiskHashDictionaryIterator', ':DictionaryReader']
)
indexlib_cc_library(
    name='DefaultTermDictionaryReader',
    deps=[
        ':DictionaryReader',
        '//aios/storage/indexlib/index/inverted_index/format:DictInlineEncoder',
        '//aios/storage/indexlib/index/inverted_index/format:ShortListOptimizeUtil'
    ]
)
indexlib_cc_library(
    name='CommonDiskHashDictionaryIterator',
    srcs=[],
    deps=[':DictionaryIterator']
)
indexlib_cc_library(
    name='CommonDiskTieredDictionaryReader',
    srcs=[],
    deps=[
        ':CommonDiskTieredDictionaryIterator', ':DictionaryReader',
        '//aios/storage/indexlib/file_system/stream:file_stream'
    ]
)
indexlib_cc_library(
    name='CommonDiskTieredDictionaryIterator',
    srcs=[],
    deps=[':DictionaryIterator']
)
indexlib_cc_library(
    name='HashDictionaryReader',
    srcs=[],
    deps=[
        ':BlockHashDictionaryReader', ':DictionaryReader',
        ':IntegrateHashDictionaryReader'
    ]
)
indexlib_cc_library(
    name='HashDictionaryWriter',
    srcs=[],
    deps=[':DictionaryWriter', '//aios/storage/indexlib/util:simple_pool']
)
indexlib_cc_library(
    name='HashDictionaryIterator', srcs=[], deps=[':DictionaryIterator']
)
indexlib_cc_library(
    name='DictionaryReader',
    deps=[
        ':DictionaryIterator', ':UtilDefine', '//aios/future_lite',
        '//aios/storage/indexlib/index/common:DictKeyInfo',
        '//aios/storage/indexlib/index/common:error_code'
    ]
)
indexlib_cc_library(
    name='DictionaryWriter',
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/index/common:DictKeyInfo',
        '//aios/storage/indexlib/index/common:Types'
    ]
)
indexlib_cc_library(
    name='DictionaryIterator',
    srcs=[],
    deps=['//aios/autil:log', '//aios/storage/indexlib/index/common:Types']
)
