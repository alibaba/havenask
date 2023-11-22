load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
load('//aios/storage:defs.bzl', 'strict_cc_library')
strict_cc_fast_test(
    name='hash_table_test',
    srcs=[
        'ChainHashTableTest.cpp', 'CuckooHashTableTest.cpp',
        'DenseHashTableTest.cpp', 'HashTableReaderTest.cpp',
        'HashTableWriterTest.cpp', 'SeparateChainHashTableTest.cpp'
    ],
    copts=['-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    shard_count=2,
    deps=[
        ':HashTableTestHelper', '//aios/storage/indexlib/file_system/mock',
        '//aios/storage/indexlib/file_system/test:test-util',
        '//aios/storage/indexlib/index/common/hash_table',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
strict_cc_fast_test(
    name='hash_table_perf_test',
    srcs=[
        'CuckooHashTableMultiThreadTest.cpp',
        'DenseHashTableMultiThreadTest.cpp'
    ],
    copts=['-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    shard_count=2,
    deps=[
        ':HashTableTestHelper', '//aios/storage/indexlib/file_system/mock',
        '//aios/storage/indexlib/file_system/test:test-util',
        '//aios/storage/indexlib/index/common/hash_table',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
strict_cc_library(
    name='HashTableTestHelper', srcs=[], hdrs=['HashTableTestHelper.h']
)
