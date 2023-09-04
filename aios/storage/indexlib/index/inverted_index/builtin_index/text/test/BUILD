load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
load('//bazel:defs.bzl', 'if_clang')
strict_cc_fast_test(
    name='text_index_test',
    srcs=['TextIndexReaderTest.cpp'],
    copts=(['-fno-access-control'] + if_clang(['-std=c++20'])),
    shard_count=2,
    deps=[
        '//aios/storage/indexlib/index/inverted_index:InvertedDiskIndexer',
        '//aios/storage/indexlib/index/inverted_index:InvertedIndexReaderImpl',
        '//aios/storage/indexlib/index/inverted_index:InvertedMemIndexer',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/test_util:InvertedTestUtil',
        '//aios/storage/indexlib/index/inverted_index/config/test:InvertedIndexConfigCreator',
        '//aios/storage/indexlib/index/inverted_index/test:InvertedTestHelper',
        '//aios/unittest_framework'
    ]
)
