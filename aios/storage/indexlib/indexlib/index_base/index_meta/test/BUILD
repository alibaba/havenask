load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_index_meta_test_helper',
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/index_base/index_meta/test'
)
cc_fast_test(
    name='indexlib_index_meta_unittest',
    srcs=glob(['*.cpp']),
    copts=['-fno-access-control', '-Werror'],
    data=(['//aios/storage/indexlib:testdata'] + glob(['testdata/**'])),
    shard_count=1,
    deps=[
        ':indexlib_index_meta_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/config/test:indexlib_config_test_helper',
        '//aios/storage/indexlib/indexlib/index/merger_util/truncate/test:indexlib_truncate_test_helper',
        '//aios/storage/indexlib/indexlib/index_base',
        '//aios/storage/indexlib/indexlib/test:directory-creator'
    ]
)
