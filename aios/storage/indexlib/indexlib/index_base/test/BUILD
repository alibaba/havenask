load('//bazel:defs.bzl', 'cc_fast_test')
load('//bazel:defs.bzl', 'cc_fast_test')
load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
cc_library(
    name='indexlib_index_base_test_helper',
    hdrs=glob(['*h']),
    copts=['-Werror'],
    include_prefix='indexlib/index_base/test'
)
cc_fast_test(
    name='indexlib_index_base_unittest',
    srcs=[
        'branch_fs_unittest.cpp', 'merge_task_resource_manager_unittest.cpp',
        'offline_recover_strategy_unittest.cpp', 'schema_adapter_unittest.cpp',
        'schema_rewriter_unittest.cpp', 'version_committer_unittest.cpp'
    ],
    copts=['-Werror', '-fno-access-control'],
    data=(['//aios/storage/indexlib:testdata'] + glob(['testdata/**'])),
    shard_count=2,
    deps=[
        ':indexlib_index_base_test_helper',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system/test:test-util',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index_base',
        '//aios/storage/indexlib/indexlib/test:partition_state_machine'
    ]
)
cc_fast_test(
    name='indexlib_index_base_exception_unittest',
    srcs=['deploy_index_wrapper_unittest.cpp'],
    copts=['-Werror', '-fno-access-control'],
    data=(['//aios/storage/indexlib:testdata'] + glob(['testdata/**'])),
    shard_count=1,
    deps=[
        ':indexlib_index_base_test_helper',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system/test:test-util',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index_base',
        '//aios/storage/indexlib/indexlib/test:partition_state_machine'
    ]
)
strict_cc_fast_test(
    name='LifecycleStrategyTest',
    srcs=['LifecycleStrategyTest.cpp'],
    deps=[
        '//aios/storage/indexlib/framework:Version',
        '//aios/storage/indexlib/indexlib/index_base',
        '//aios/unittest_framework'
    ]
)
strict_cc_fast_test(
    name='tablet_deployer_test',
    srcs=['tablet_deployer_test.cpp'],
    deps=[
        '//aios/storage/indexlib/framework:TabletDeployer',
        '//aios/storage/indexlib/indexlib/index_base',
        '//aios/unittest_framework'
    ]
)
cc_fast_test(
    name='legacy_schema_adaptor_unittest',
    srcs=['legacy_schema_adaptor_unittest.cpp'],
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    shard_count=2,
    deps=[
        ':indexlib_index_base_test_helper',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system/test:test-util',
        '//aios/storage/indexlib/index/attribute:constants',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index_base',
        '//aios/storage/indexlib/indexlib/test:partition_state_machine'
    ]
)
