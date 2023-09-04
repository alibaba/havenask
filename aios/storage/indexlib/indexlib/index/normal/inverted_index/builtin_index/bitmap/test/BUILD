load('@rules_cc//cc:defs.bzl', 'cc_library')
load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_bitmap_test_helper',
    testonly=True,
    srcs=glob(['*.cpp'], exclude=['*unittest.cpp']),
    hdrs=glob(['*h']),
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/inverted_index/builtin_index/bitmap/test',
    visibility=[
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap/test:__pkg__'
    ],
    deps=[
        '//aios/storage/indexlib/index/inverted_index/builtin_index/bitmap:BitmapIndexWriter',
        '//aios/storage/indexlib/indexlib:indexlib_testbase_helper',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper',
        '//aios/storage/indexlib/indexlib/test:directory-creator'
    ]
)
cc_fast_test(
    name='indexlib_bitmap_unittest',
    srcs=glob(['*unittest.cpp']),
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    deps=[
        ':indexlib_bitmap_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase'
    ]
)
