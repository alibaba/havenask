package(default_visibility=['//aios/storage/indexlib/indexlib:__subpackages__'])
load('//bazel:defs.bzl', 'cc_fast_test', 'cc_slow_test')
cc_library(
    name='indexlib_partition_test_helper',
    testonly=True,
    srcs=[
        'fake_index_partition_creator.cpp', 'fake_open_executor_chain.cpp',
        'index_partition_maker.cpp'
    ],
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/partition/test',
    deps=[
        '//aios/storage/indexlib/indexlib:indexlib_testbase_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testlib',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper',
        '//aios/storage/indexlib/indexlib/partition:indexlib_partition',
        '//aios/storage/indexlib/indexlib/util/test:indexlib_util_test_helper'
    ]
)
cc_library(
    name='main-sub-test-util',
    testonly=True,
    srcs=['doc_id_manager_test_util.cpp', 'main_sub_test_util.cpp'],
    hdrs=['doc_id_manager_test_util.h', 'main_sub_test_util.h'],
    copts=['-Werror'],
    include_prefix='indexlib/partition/test',
    deps=[
        '//aios/storage/indexlib/index/primary_key/mock:MockPrimaryKeyIndexReader',
        '//aios/storage/indexlib/indexlib:indexlib_testbase_helper',
        '//aios/storage/indexlib/indexlib/index/normal/primarykey/test:primarykey_test_helper',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper',
        '//aios/storage/indexlib/indexlib/partition:indexlib_partition'
    ]
)
TEST_COMMON_DEPS = [
    ':indexlib_partition_test_helper',
    '//aios/storage/indexlib/indexlib/partition/operation_queue/test:indexlib_operation_queue_test_helper',
    '//aios/storage/indexlib/indexlib/merger/test:indexlib_merger_test_helper',
    '//aios/storage/indexlib/indexlib/util/test:indexlib_util_test_helper',
    '//aios/storage/indexlib/indexlib/partition/segment/test:indexlib_segment_test_helper',
    '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper',
    '//aios/storage/indexlib/indexlib/config/test:indexlib_config_test_helper',
    '//aios/storage/indexlib/indexlib:indexlib_testbase',
    '//aios/storage/indexlib/indexlib/test:partition_state_machine',
    '//aios/storage/indexlib/indexlib:indexlib_testlib',
    '//aios/storage/indexlib/framework:ITablet',
    '//aios/storage/indexlib/framework:Tablet',
    '//aios/storage/indexlib/framework:ITabletReader',
    '//aios/storage/indexlib/framework:VersionDeployDescription',
    '//aios/storage/indexlib/framework:TabletDeployer',
    '//aios/storage/indexlib/base:Status',
    '//aios/storage/indexlib/config:TabletSchema',
    '//aios/storage/indexlib/table/normal_table:NormalTabletSessionReader',
    '//aios/storage/indexlib/table/normal_table:table'
]
cc_fast_test(
    name='partition-test',
    srcs=[
        'aux_table_reader_unittest.cpp', 'doc_range_partitioner_unittest.cpp',
        'flushed_locator_container_unittest.cpp',
        'in_mem_virtual_attribute_cleaner_unittest.cpp',
        'in_memory_index_cleaner_unittest.cpp',
        'in_memory_partition_data_unittest.cpp',
        'index_application_unittest.cpp', 'index_format_version_unittest.cpp',
        'join_segment_writer_unittest.cpp', 'lifecycle_load_intetest.cpp',
        'local_index_cleaner_unittest.cpp',
        'memory_stat_collector_unittest.cpp', 'offline_partition_intetest.cpp',
        'offline_partition_reader_unittest.cpp',
        'offline_partition_unittest.cpp',
        'offline_partition_writer_unittest.cpp',
        'on_disk_index_cleaner_unittest.cpp',
        'on_disk_partition_data_unittest.cpp', 'online_partition_unittest.cpp',
        'online_partition_writer_unittest.cpp',
        'open_reader_by_loadconfig_unittest.cpp',
        'partition_counter_intetest.cpp',
        'partition_group_resource_unittest.cpp',
        'partition_reader_cleaner_unittest.cpp',
        'partition_reader_snapshot_unittest.cpp',
        'partition_resource_calculator_unittest.cpp',
        'partition_validater_unittest.cpp',
        'partition_writer_resource_calculator_unittest.cpp',
        'raw_document_field_extractor_unittest.cpp',
        'reader_container_unittest.cpp', 'reader_snapshot_unittest.cpp',
        'realtime_partition_data_reclaimer_unittest.cpp',
        'reopen_decider_unittest.cpp', 'segment_metrics_updater_intetest.cpp',
        'sort_build_checker_unittest.cpp',
        'table_reader_container_updater_unittest.cpp',
        'time_attribute_unittest.cpp', 'time_range_index_intetest.cpp'
    ],
    copts=['-fno-access-control', '-Werror'],
    data=(['//aios/storage/indexlib:testdata'] + glob(['testdata/**'])),
    flaky=True,
    shard_count=10,
    deps=TEST_COMMON_DEPS
)
cc_fast_test(
    name='partition-test-private-api-access',
    srcs=[
        'aux_table_intetest.cpp', 'build_document_metrics_unittest.cpp',
        'lifecycle_load_unittest.cpp', 'main_sub_doc_id_manager_unittest.cpp',
        'offline_branch_build_unittest.cpp', 'patch_loader_unittest.cpp',
        'schema_rewriter_unittest.cpp', 'sub_doc_unittest.cpp'
    ],
    copts=['-Werror', '-fno-access-control'],
    data=(['//aios/storage/indexlib:testdata'] + glob(['testdata/**'])),
    shard_count=5,
    deps=(
        TEST_COMMON_DEPS + [
            '//aios/storage/indexlib/indexlib/index/normal/primarykey/test:primarykey_test_helper',
            ':main-sub-test-util'
        ]
    )
)
cc_fast_test(
    name='patch-modifier-test',
    srcs=['patch_modifier_unittest.cpp'],
    copts=['-fno-access-control', '-Werror'],
    deps=TEST_COMMON_DEPS
)
cc_slow_test(
    name='todo',
    srcs=[],
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    deps=TEST_COMMON_DEPS
)
genrule(
    name='cp_codegen_package',
    srcs=['//aios/storage/indexlib/indexlib:codegen_package_tar'],
    outs=['testdata/codegen_package.tar'],
    cmd='cp $(locations //aios/storage/indexlib/indexlib:codegen_package_tar) $@'
)
genrule(
    name='cp_codegen_headers_package',
    srcs=['//aios/storage/indexlib/indexlib:indexlib_codegen_header_package'],
    outs=['testdata/indexlib_codegen_header.tar'],
    cmd='cp $(locations //aios/storage/indexlib/indexlib:indexlib_codegen_header_package) $@'
)
cc_fast_test(
    name='indexlib_partition_kkv_unittest',
    srcs=['kkv_table_intetest.cpp', 'multi_region_kkv_intetest.cpp'],
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    flaky=True,
    shard_count=10,
    deps=TEST_COMMON_DEPS
)
cc_fast_test(
    name='indexlib_partition_kv_unittest',
    srcs=['kv_table_intetest.cpp', 'multi_region_kv_intetest.cpp'],
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    deps=TEST_COMMON_DEPS
)
cc_fast_test(
    name='online-partition-test-1',
    srcs=[
        'auto_add2update_intetest.cpp', 'number_range_index_intetest.cpp',
        'online_offline_join_intetest.cpp', 'recover_rt_index_unittest.cpp',
        'search_cache_intetest.cpp'
    ],
    copts=['-Werror', '-fno-access-control'],
    data=(['//aios/storage/indexlib:testdata'] + glob(['testdata/**'])),
    flaky=True,
    shard_count=2,
    deps=TEST_COMMON_DEPS
)
cc_fast_test(
    name='online-partition-test-2',
    srcs=[
        'force_reopen_intetest.cpp', 'online_partition_intetest.cpp',
        'online_partition_reader_intetest.cpp'
    ],
    copts=['-Werror', '-fno-access-control'],
    data=(['//aios/storage/indexlib:testdata'] + glob(['testdata/**'])),
    flaky=True,
    shard_count=2,
    deps=TEST_COMMON_DEPS
)
cc_fast_test(
    name='inplace-modifier-test',
    srcs=['inplace_modifier_unittest.cpp'],
    copts=['-Werror', '-fno-access-control'],
    deps=(
        TEST_COMMON_DEPS + [
            '//aios/storage/indexlib/indexlib/index/normal/primarykey/test:primarykey_test_helper',
            ':main-sub-test-util'
        ]
    )
)
cc_fast_test(
    name='doc-id-manager-test',
    srcs=[
        'main_sub_util_unittest.cpp', 'normal_doc_id_manager_unittest.cpp',
        'sub_doc_modifier_unittest.cpp'
    ],
    copts=['-fno-access-control', '-Werror'],
    shard_count=2,
    deps=(
        TEST_COMMON_DEPS + [
            '//aios/storage/indexlib/indexlib/index/normal/primarykey/test:primarykey_test_helper',
            ':main-sub-test-util'
        ]
    )
)
cc_fast_test(
    name='build-work-item-generator-test',
    srcs=['doc_build_work_item_generator_unittest.cpp'],
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    deps=(
        TEST_COMMON_DEPS + [
            '//aios/storage/indexlib/indexlib/index/normal/primarykey/test:primarykey_test_helper',
            ':main-sub-test-util'
        ]
    )
)
