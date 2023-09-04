load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=[':__subpackages__'])
strict_cc_library(
    name='SKeyCollectInfo',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/index/kkv:constants',
        '//aios/storage/indexlib/index/kkv/common:KKVDoc'
    ]
)
strict_cc_library(name='SKeyCollectInfoPool', srcs=[], deps=[])
strict_cc_library(
    name='KKVDocFieldComparator',
    srcs=[],
    deps=[
        'SKeyCollectInfo',
        '//aios/storage/indexlib/index/common/field_format/pack_attribute'
    ]
)
strict_cc_library(name='SKeyHashComparator', srcs=[], deps=[':SKeyCollectInfo'])
strict_cc_library(
    name='KKVDocComparator',
    deps=[
        ':KKVDocFieldComparator', ':SKeyCollectInfo',
        '//aios/storage/indexlib/index/common/field_format/pack_attribute',
        '//aios/storage/indexlib/index/kkv/common:Trait',
        '//aios/storage/indexlib/index/kkv/config'
    ]
)
strict_cc_library(name='KKVDumpPhrase', srcs=[])
strict_cc_library(
    name='KKVDocSorterBase',
    srcs=[],
    deps=[
        ':KKVDocFieldComparator', ':SKeyCollectInfo', ':SKeyCollectInfoPool',
        '//aios/storage/indexlib/index/kkv/config'
    ]
)
strict_cc_library(
    name='NormalKKVDocSorter', deps=[':KKVDocComparator', ':KKVDocSorterBase']
)
strict_cc_library(
    name='TruncateKKVDocSorter',
    deps=[
        ':KKVDocComparator', ':KKVDocSorterBase', ':SKeyHashComparator',
        '//aios/storage/indexlib/index/kkv/common:Trait'
    ]
)
strict_cc_library(
    name='KKVDocSorterFactory',
    srcs=[],
    deps=[':KKVDumpPhrase', ':NormalKKVDocSorter', ':TruncateKKVDocSorter']
)
strict_cc_library(
    name='KKVFileWriterOptionHelper',
    srcs=[],
    visibility=['//aios/storage/indexlib/index/kkv:__subpackages__'],
    deps=['//aios/storage/indexlib/index/kkv/config']
)
strict_cc_library(
    name='PKeyDumper',
    deps=[
        '//aios/storage/indexlib/index/kkv/common:ChunkDefine',
        '//aios/storage/indexlib/index/kkv/common:OnDiskPKeyOffset',
        '//aios/storage/indexlib/index/kkv/common:SKeyListInfo',
        '//aios/storage/indexlib/index/kkv/config',
        '//aios/storage/indexlib/index/kkv/pkey_table'
    ]
)
strict_cc_library(
    name='KKVValueDumper',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/index/kkv:constants',
        '//aios/storage/indexlib/index/kkv/common:ChunkWriter',
        '//aios/storage/indexlib/index/kkv/common:NormalOnDiskSKeyNode'
    ]
)
strict_cc_library(
    name='SKeyDumperBase',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/index/kkv:constants',
        '//aios/storage/indexlib/index/kkv/common:InlineOnDiskSKeyNode',
        '//aios/storage/indexlib/index/kkv/common:NormalOnDiskSKeyNode'
    ]
)
strict_cc_library(
    name='InlineSKeyDumper',
    srcs=[],
    deps=[
        ':SKeyDumperBase',
        '//aios/storage/indexlib/index/kkv/common:ChunkWriter',
        '//aios/storage/indexlib/index/kkv/common:InlineOnDiskSKeyNode'
    ]
)
strict_cc_library(
    name='NormalSKeyDumper',
    srcs=[],
    deps=[
        ':SKeyDumperBase',
        '//aios/storage/indexlib/index/kkv/common:ChunkWriter',
        '//aios/storage/indexlib/index/kkv/common:NormalOnDiskSKeyNode'
    ]
)
strict_cc_library(
    name='KKVDataDumperBase',
    deps=[
        ':KKVDocSorterFactory', ':KKVDumpPhrase', ':PKeyDumper',
        '//aios/storage/indexlib/index/kkv/common:KKVIndexFormat',
        '//aios/storage/indexlib/index/kkv/config'
    ]
)
strict_cc_library(
    name='InlineKKVDataDumper',
    srcs=[],
    deps=[':InlineSKeyDumper', ':KKVDataDumperBase']
)
strict_cc_library(
    name='NormalKKVDataDumper',
    srcs=[],
    deps=[':KKVDataDumperBase', ':KKVValueDumper', ':NormalSKeyDumper']
)
strict_cc_library(
    name='KKVDataDumperFactory',
    srcs=[],
    deps=[':InlineKKVDataDumper', ':NormalKKVDataDumper']
)
strict_cc_library(
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
strict_cc_library(
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
