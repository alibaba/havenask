load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
load('//bazel:defs.bzl', 'if_clang')
strict_cc_fast_test(
    name='date_index_test',
    srcs=['DateLeafReaderTest.cpp', 'TimeRangeInfoTest.cpp'],
    copts=(['-fno-access-control'] + if_clang(['-std=c++20'])),
    shard_count=5,
    deps=[
        '//aios/storage/indexlib/index/inverted_index/builtin_index/date:DateLeafReader',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/date:TimeRangeInfo',
        '//aios/unittest_framework'
    ]
)
