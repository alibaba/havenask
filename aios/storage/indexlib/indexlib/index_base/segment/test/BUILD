load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_segment_test_helper',
    hdrs=glob(['*h']),
    copts=['-Werror'],
    include_prefix='indexlib/index_base/segment/test',
    visibility=['//aios/storage/indexlib:__subpackages__']
)
cc_fast_test(
    name='indexlib_segment_unittest',
    srcs=glob(['*unittest.cpp']),
    copts=['-fno-access-control', '-Werror'],
    data=['//aios/storage/indexlib:testdata'],
    shard_count=2,
    deps=[
        ':indexlib_segment_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index_base',
        '//aios/storage/indexlib/indexlib/partition/segment/test:indexlib_segment_test_helper',
        '//aios/storage/indexlib/indexlib/test:index_meta_maker'
    ]
)
