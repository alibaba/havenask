load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='geo_hash_unittest',
    srcs=['GeoHashUtilTest.cpp'],
    copts=['-fno-access-control'],
    shard_count=5,
    deps=[
        '//aios/storage/indexlib/index/common/field_format/spatial:shape',
        '//aios/unittest_framework'
    ]
)
