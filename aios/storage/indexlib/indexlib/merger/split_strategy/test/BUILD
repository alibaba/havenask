load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_split_strategy_test_helper',
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/merger/split_strategy/test'
)
cc_fast_test(
    name='indexlib_split_strategy_unittest',
    srcs=glob(['*.cpp']),
    copts=['-Werror', '-fno-access-control'],
    data=(['//aios/storage/indexlib:testdata'] + glob(['testdata/**'])),
    deps=[
        ':indexlib_split_strategy_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index/merger_util/truncate/test:indexlib_truncate_test_helper',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper',
        '//aios/storage/indexlib/indexlib/merger/test:indexlib_merger_test_helper'
    ]
)
