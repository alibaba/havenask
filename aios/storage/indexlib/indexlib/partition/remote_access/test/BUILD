load('//bazel:defs.bzl', 'cc_fast_test', 'copy_target_to')
cc_library(
    name='indexlib_remote_access_test_helper',
    hdrs=glob(['*h']),
    copts=['-Werror'],
    include_prefix='indexlib/partition/remote_access/test'
)
copy_target_to(
    name='demo_indexer_testdata',
    testonly=True,
    copy_name='libdemo_indexer.so',
    dests=['testdata/demo_indexer_plugins/'],
    to_copy='//aios/storage/indexlib/indexlib/index/normal/inverted_index/customized_index/test:demo_indexer.so'
)
cc_fast_test(
    name='indexlib_remote_access_unittest',
    srcs=[
        'attribute_data_patcher_unittest.cpp', 'modify_operation_intetest.cpp',
        'new_schema_drop_field_unittest.cpp',
        'partition_resource_provider_intetest.cpp',
        'var_num_patch_data_writer_unittest.cpp'
    ],
    copts=['-Werror', '-fno-access-control'],
    data=([':demo_indexer_testdata'] + glob(['testdata/**'])),
    shard_count=10,
    deps=[
        ':indexlib_remote_access_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib:indexlib_testlib',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper',
        '//aios/storage/indexlib/indexlib/partition:index_roll_back_util',
        '//aios/storage/indexlib/indexlib/partition/test:indexlib_partition_test_helper'
    ]
)
