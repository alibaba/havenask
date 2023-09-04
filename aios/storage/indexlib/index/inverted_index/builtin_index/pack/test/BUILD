load('//aios/storage:defs.bzl', 'strict_cc_fast_test', 'strict_cc_library')
load('//bazel:defs.bzl', 'if_clang')
strict_cc_library(
    name='PackTestHelper',
    testonly=True,
    srcs=['PackPositionMaker.cpp', 'PackPostingMaker.cpp'],
    hdrs=['PackPositionMaker.h', 'PackPostingMaker.h'],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/index/inverted_index:PostingWriterImpl',
        '//aios/storage/indexlib/index/inverted_index/test:PostingMaker'
    ]
)
strict_cc_fast_test(
    name='pack_index_test',
    srcs=['PackIndexReaderTest.cpp', 'PackPostingMergerTest.cpp'],
    copts=(['-fno-access-control'] + if_clang(['-std=c++20'])),
    shard_count=20,
    deps=[
        ':PackTestHelper',
        '//aios/storage/indexlib/framework:tablet_schema_loader',
        '//aios/storage/indexlib/framework/mock:FakeSegment',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/index/inverted_index:InvertedDiskIndexer',
        '//aios/storage/indexlib/index/inverted_index:InvertedIndexReaderImpl',
        '//aios/storage/indexlib/index/inverted_index:InvertedMemIndexer',
        '//aios/storage/indexlib/index/inverted_index:OnDiskIndexIteratorCreator',
        '//aios/storage/indexlib/index/inverted_index:PostingMergerImpl',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/pack:OnDiskPackIndexIterator',
        '//aios/storage/indexlib/index/inverted_index/builtin_index/test_util:InvertedTestUtil',
        '//aios/storage/indexlib/index/inverted_index/config/test:InvertedIndexConfigCreator',
        '//aios/storage/indexlib/index/inverted_index/test:InvertedTestHelper',
        '//aios/storage/indexlib/index/inverted_index/test:PostingMaker',
        '//aios/storage/indexlib/index/test:FakeDocMapper',
        '//aios/storage/indexlib/index/test:IndexTestUtil',
        '//aios/storage/indexlib/table/normal_table:table',
        '//aios/unittest_framework'
    ]
)
