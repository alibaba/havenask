load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='test-headers',
    testonly=1,
    hdrs=['fake_index_partition_unittest.h'],
    copts=['-Werror'],
    include_prefix='indexlib/testlib/test'
)
cc_fast_test(
    name='unittest',
    srcs=[
        'fake_index_partition_unittest.cpp', 'IndexlibPartitionCreatorTest.cpp'
    ],
    copts=['-Werror'],
    data=['//aios/storage/indexlib:testdata'],
    deps=[
        ':test-headers', '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib:indexlib_testlib',
        '//aios/storage/indexlib/indexlib/testlib'
    ]
)
