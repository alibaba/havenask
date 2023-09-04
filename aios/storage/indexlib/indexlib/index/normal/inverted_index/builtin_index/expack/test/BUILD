load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_expack_test_helper',
    testonly=True,
    srcs=glob(['*.cpp'], exclude=['*unittest.cpp']),
    hdrs=glob(['*h']),
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/inverted_index/builtin_index/expack/test',
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/indexlib:indexlib_testbase_helper',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper'
    ]
)
cc_fast_test(
    name='indexlib_expack_unittest',
    srcs=glob(['*unittest.cpp']),
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    shard_count=5,
    deps=[
        ':indexlib_expack_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index/normal/attribute/test:indexlib_attribute_test_helper',
        '//aios/storage/indexlib/indexlib/index/normal/inverted_index/builtin_index/pack/test:indexlib_pack_test_helper'
    ]
)
