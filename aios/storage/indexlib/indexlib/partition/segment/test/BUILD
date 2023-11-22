load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_segment_test_helper',
    testonly=True,
    hdrs=glob(['*h']),
    copts=['-Werror'],
    include_prefix='indexlib/partition/segment/test',
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/indexlib',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper'
    ]
)
cc_fast_test(
    name='indexlib_segment_unittest',
    srcs=glob(['*.cpp']),
    copts=['-fno-access-control', '-Werror'],
    data=(['//aios/storage/indexlib:testdata'] + glob(['testdata/**'])),
    shard_count=5,
    deps=[
        ':indexlib_segment_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper',
        '//aios/storage/indexlib/indexlib/partition/test:main-sub-test-util'
    ]
)
