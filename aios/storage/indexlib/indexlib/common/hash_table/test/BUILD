load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_hash_table_test_helper',
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/common/hash_table/test',
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__']
)
cc_fast_test(
    name='indexlib_hash_table_unittest',
    srcs=glob(['*.cpp']),
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    shard_count=2,
    deps=[
        ':indexlib_hash_table_test_helper',
        '//aios/storage/indexlib/file_system/mock',
        '//aios/storage/indexlib/file_system/test:test-util',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/common'
    ]
)
