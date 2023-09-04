load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_config_test_helper',
    srcs=glob(['*.cpp'], exclude=['*unittest.cpp']),
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/config/test',
    visibility=['//visibility:public'],
    deps=['//aios/storage/indexlib/indexlib/config']
)
cc_fast_test(
    name='indexlib_config_unittest',
    srcs=glob(['*unittest.cpp']),
    copts=['-Werror', '-fno-access-control'],
    data=(['//aios/storage/indexlib:testdata'] + glob(['testdata/**'])),
    shard_count=2,
    deps=[
        ':indexlib_config_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib:indexlib_testlib',
        '//aios/storage/indexlib/indexlib/config'
    ]
)
