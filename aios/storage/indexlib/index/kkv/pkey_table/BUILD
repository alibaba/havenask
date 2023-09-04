load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=[':__subpackages__'])
strict_cc_library(
    name='pkey_table',
    srcs=['OnDiskSeparateChainHashIterator.cpp'],
    hdrs=[
        'ClosedHashPrefixKeyTable.h', 'ClosedHashPrefixKeyTableIterator.h',
        'ClosedHashPrefixKeyTableTraits.h', 'NonDeleteValue.h',
        'OnDiskClosedHashIterator.h', 'OnDiskPKeyHashIteratorCreator.h',
        'OnDiskSeparateChainHashIterator.h', 'PKeyTableType.h',
        'PrefixKeyTableBase.h', 'PrefixKeyTableCreator.h',
        'PrefixKeyTableIteratorBase.h', 'PrefixKeyTableIteratorTyped.h',
        'PrefixKeyTableSeeker.h', 'SeparateChainPrefixKeyTable.h',
        'SeparateChainPrefixKeyTableIterator.h'
    ],
    visibility=['//aios/storage/indexlib/index/kkv:__subpackages__'],
    deps=[
        '//aios/autil:log', '//aios/autil:time',
        '//aios/storage/indexlib/base:NoExceptionWrapper',
        '//aios/storage/indexlib/index/common/hash_table',
        '//aios/storage/indexlib/index/kkv:Types',
        '//aios/storage/indexlib/index/kkv:constants',
        '//aios/storage/indexlib/index/kkv/common:KKVMetricsCollector',
        '//aios/storage/indexlib/index/kkv/common:OnDiskPKeyOffset',
        '//aios/storage/indexlib/index/kkv/common:SKeyListInfo',
        '//aios/storage/indexlib/index/kkv/config'
    ]
)
