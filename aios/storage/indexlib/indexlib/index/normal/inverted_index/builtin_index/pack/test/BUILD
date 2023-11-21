load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_pack_test_helper',
    testonly=True,
    srcs=['pack_position_maker.cpp', 'pack_posting_maker.cpp'],
    hdrs=glob(['*h']),
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/inverted_index/builtin_index/pack/test',
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/index/inverted_index:PostingWriterImpl',
        '//aios/storage/indexlib/indexlib',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper'
    ]
)
cc_fast_test(
    name='indexlib_pack_unittest',
    srcs=glob(['*unittest.cpp']),
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    shard_count=5,
    deps=[
        ':indexlib_pack_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index/normal/attribute/test:indexlib_attribute_test_helper'
    ]
)
