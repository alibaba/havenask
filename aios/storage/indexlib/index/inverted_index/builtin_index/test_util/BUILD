load('//aios/storage:defs.bzl', 'strict_cc_library')
load('//bazel:defs.bzl', 'if_clang')
strict_cc_library(
    name='InvertedTestUtil',
    testonly=True,
    copts=(['-fno-access-control'] + if_clang(['-std=c++20'])),
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/index/inverted_index:InvertedIndexReaderImpl',
        '//aios/storage/indexlib/index/inverted_index:InvertedMemIndexer'
    ]
)
