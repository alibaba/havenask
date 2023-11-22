load('//bazel:defs.bzl', 'cc_fast_test')
cc_fast_test(
    name='indexlib_modifier_unittest',
    srcs=glob(['*.cpp', '*.h']),
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    shard_count=5,
    deps=[
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper',
        '//aios/storage/indexlib/indexlib/partition/modifier'
    ]
)
