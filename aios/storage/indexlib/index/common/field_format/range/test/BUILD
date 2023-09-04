load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='range_field_encoder_test',
    srcs=['RangeFieldEncoderTest.cpp'],
    copts=['-fno-access-control'],
    shard_count=5,
    deps=[
        '//aios/storage/indexlib/index/common/field_format/range:RangeFieldEncoder',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
