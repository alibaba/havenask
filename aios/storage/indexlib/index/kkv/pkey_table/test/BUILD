load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='indexlib_kkv_hash_test',
    srcs=[
        'ClosedHashPrefixKeyTableTest.cpp', 'OnDiskClosedHashIteratorTest.cpp',
        'OnDiskSeparateChainHashIteratorTest.cpp',
        'SeparateChainPrefixKeyTableTest.cpp'
    ],
    copts=['-fno-access-control'],
    shard_count=3,
    deps=[
        '//aios/storage/indexlib/config:schema',
        '//aios/storage/indexlib/index/kkv/pkey_table',
        '//aios/storage/indexlib/table/kkv_table/test:KKVTabletSchemaMaker',
        '//aios/unittest_framework'
    ]
)
