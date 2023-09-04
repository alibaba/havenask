load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_truncate_test_helper',
    testonly=True,
    srcs=['fake_single_truncate_index_writer.cpp', 'truncate_test_helper.cpp'],
    hdrs=glob(['*h']),
    copts=['-Werror'],
    include_prefix='indexlib/index/merger_util/truncate/test',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/indexlib',
        '//aios/storage/indexlib/indexlib:indexlib_testbase_helper',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper'
    ]
)
cc_fast_test(
    name='indexlib_truncate_unittest',
    srcs=glob(['*unittest.cpp', '*intetest.cpp'],
              exclude=[
                  'posting_truncator_unittest.cpp',
                  'truncate_doc_collector_unittest.cpp'
              ]),
    copts=['-Werror', '-fno-access-control'],
    data=glob(['testdata/**']),
    flaky=True,
    shard_count=10,
    deps=[
        ':indexlib_truncate_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/config/test:indexlib_config_test_helper',
        '//aios/storage/indexlib/indexlib/index/normal/attribute/test:indexlib_attribute_test_helper',
        '//aios/storage/indexlib/indexlib/index/normal/inverted_index/builtin_index/pack/test:indexlib_pack_test_helper'
    ]
)
