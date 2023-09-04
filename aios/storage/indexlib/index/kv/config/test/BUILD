load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='kv_index_config_test',
    srcs=['KVIndexConfigTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        '//aios/storage/indexlib/index/kv/config:KVIndexConfig',
        '//aios/storage/indexlib/index/kv/config:KVIndexPreference',
        '//aios/unittest_framework'
    ]
)
