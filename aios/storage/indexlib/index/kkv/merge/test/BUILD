load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='indexlib_merge_test',
    srcs=['OnDiskKKVIteratorTest.cpp'],
    copts=['-fno-access-control'],
    shard_count=2,
    deps=[
        '//aios/future_lite/future_lite/executors:simple_executor',
        '//aios/storage/indexlib/config:schema',
        '//aios/storage/indexlib/document',
        '//aios/storage/indexlib/document/test:KVDocumentBatchMaker',
        '//aios/storage/indexlib/index/kkv:KKVIndexFactory',
        '//aios/storage/indexlib/index/kkv/building:kkv_mem_indexer',
        '//aios/storage/indexlib/index/kkv/config/test:kkv_index_config_builder',
        '//aios/storage/indexlib/table/kkv_table/test:KKVTabletSchemaMaker',
        '//aios/unittest_framework'
    ]
)
