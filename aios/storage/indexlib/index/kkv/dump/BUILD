load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=[':__subpackages__'])
indexlib_cc_library(
    name='SKeyCollectInfo',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/index/kkv:constants',
        '//aios/storage/indexlib/index/kkv/common:KKVDoc'
    ]
)
indexlib_cc_library(name='SKeyCollectInfoPool', srcs=[], deps=[])
indexlib_cc_library(
    name='KKVDocFieldComparator',
    srcs=[],
    deps=[
        'SKeyCollectInfo',
        '//aios/storage/indexlib/index/common/field_format/pack_attribute'
    ]
)
indexlib_cc_library(
    name='SKeyHashComparator', srcs=[], deps=[':SKeyCollectInfo']
)
indexlib_cc_library(
    name='KKVDocComparator',
    deps=[
        ':KKVDocFieldComparator', ':SKeyCollectInfo',
        '//aios/storage/indexlib/index/common/field_format/pack_attribute',
        '//aios/storage/indexlib/index/kkv/common:Trait',
        '//aios/storage/indexlib/index/kkv/config'
    ]
)
indexlib_cc_library(name='KKVDumpPhrase', srcs=[])
indexlib_cc_library(
    name='KKVDocSorterBase',
    srcs=[],
    deps=[
        ':KKVDocFieldComparator', ':SKeyCollectInfo', ':SKeyCollectInfoPool',
        '//aios/storage/indexlib/index/kkv/config'
    ]
)
indexlib_cc_library(
    name='NormalKKVDocSorter', deps=[':KKVDocComparator', ':KKVDocSorterBase']
)
indexlib_cc_library(
    name='TruncateKKVDocSorter',
    deps=[
        ':KKVDocComparator', ':KKVDocSorterBase', ':SKeyHashComparator',
        '//aios/storage/indexlib/index/kkv/common:Trait'
    ]
)
indexlib_cc_library(
    name='KKVDocSorterFactory',
    srcs=[],
    deps=[':KKVDumpPhrase', ':NormalKKVDocSorter', ':TruncateKKVDocSorter']
)
indexlib_cc_library(
    name='KKVFileWriterOptionHelper',
    srcs=[],
    visibility=['//aios/storage/indexlib/index/kkv:__subpackages__'],
    deps=['//aios/storage/indexlib/index/kkv/config']
)
indexlib_cc_library(
    name='PKeyDumper',
    deps=[
        '//aios/storage/indexlib/index/kkv/common:ChunkDefine',
        '//aios/storage/indexlib/index/kkv/common:OnDiskPKeyOffset',
        '//aios/storage/indexlib/index/kkv/common:SKeyListInfo',
        '//aios/storage/indexlib/index/kkv/config',
        '//aios/storage/indexlib/index/kkv/pkey_table'
    ]
)
indexlib_cc_library(
    name='KKVValueDumper',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/index/kkv:constants',
        '//aios/storage/indexlib/index/kkv/common:ChunkWriter',
        '//aios/storage/indexlib/index/kkv/common:NormalOnDiskSKeyNode'
    ]
)
indexlib_cc_library(
    name='SKeyDumperBase',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/index/kkv:constants',
        '//aios/storage/indexlib/index/kkv/common:InlineOnDiskSKeyNode',
        '//aios/storage/indexlib/index/kkv/common:NormalOnDiskSKeyNode'
    ]
)
indexlib_cc_library(
    name='InlineSKeyDumper',
    srcs=[],
    deps=[
        ':SKeyDumperBase',
        '//aios/storage/indexlib/index/kkv/common:ChunkWriter',
        '//aios/storage/indexlib/index/kkv/common:InlineOnDiskSKeyNode'
    ]
)
indexlib_cc_library(
    name='NormalSKeyDumper',
    srcs=[],
    deps=[
        ':SKeyDumperBase',
        '//aios/storage/indexlib/index/kkv/common:ChunkWriter',
        '//aios/storage/indexlib/index/kkv/common:NormalOnDiskSKeyNode'
    ]
)
indexlib_cc_library(
    name='KKVDataDumperBase',
    deps=[
        ':KKVDocSorterFactory', ':KKVDumpPhrase', ':PKeyDumper',
        '//aios/storage/indexlib/index/kkv/common:KKVIndexFormat',
        '//aios/storage/indexlib/index/kkv/config'
    ]
)
indexlib_cc_library(
    name='InlineKKVDataDumper',
    srcs=[],
    deps=[':InlineSKeyDumper', ':KKVDataDumperBase']
)
indexlib_cc_library(
    name='NormalKKVDataDumper',
    srcs=[],
    deps=[':KKVDataDumperBase', ':KKVValueDumper', ':NormalSKeyDumper']
)
indexlib_cc_library(
    name='KKVDataDumperFactory',
    srcs=[],
    deps=[':InlineKKVDataDumper', ':NormalKKVDataDumper']
)
indexlib_cc_library(
    name='kkv_data_dumper',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib/index/kkv:__subpackages__'],
    deps=[
        ':InlineSKeyDumper', ':KKVDataDumperFactory', ':KKVDocSorterFactory',
        ':KKVValueDumper', ':NormalKKVDocSorter', ':NormalSKeyDumper',
        ':PKeyDumper', ':TruncateKKVDocSorter',
        '//aios/storage/indexlib/index/kkv/config'
    ]
)
indexlib_cc_library(
    name='KKVIndexDumper',
    srcs=[],
    visibility=['//aios/storage/indexlib/index/kkv:__subpackages__'],
    deps=[
        ':KKVDocSorterFactory', ':KKVFileWriterOptionHelper',
        ':kkv_data_dumper',
        '//aios/storage/indexlib/index/kkv/common:KKVIndexFormat',
        '//aios/storage/indexlib/index/kkv/config'
    ]
)
