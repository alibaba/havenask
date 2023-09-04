load('@rules_cc//cc:defs.bzl', 'cc_library')
load(
    '//bazel:defs.bzl', 'cc_fast_test', 'cc_slow_test',
    'test_cc_plugin_shared_library'
)
cc_library(
    name='indexlib_merger_test_helper',
    testonly=True,
    srcs=[
        'merge_helper.cpp', 'merge_task_maker.cpp',
        'segment_directory_creator.cpp'
    ],
    hdrs=glob(['*h']),
    copts=['-Werror'],
    include_prefix='indexlib/merger/test',
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/indexlib:indexlib_merger',
        '//aios/storage/indexlib/indexlib:indexlib_testbase_helper',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper'
    ]
)
cc_fast_test(
    name='todo',
    srcs=[],
    copts=['-Werror'],
    data=[':merger_plugin_testdata', '//aios/storage/indexlib:testdata'],
    shard_count=30,
    deps=[':indexlib_merger_test_helper']
)
cc_fast_test(
    name='indexlib_merger_unittest',
    srcs=[
        'backup_merge_unittest.cpp', 'index_merge_meta_unittest.cpp',
        'index_partition_merger_metrics_unittest.cpp',
        'index_partition_merger_unittest.cpp',
        'index_table_merge_meta_creator_unittest.cpp',
        'key_value_merge_meta_creator_unittest.cpp',
        'merge_partition_data_creator_unittest.cpp', 'merge_plan_unittest.cpp',
        'merge_task_item_creator_unittest.cpp',
        'merge_task_item_dispatcher_unittest.cpp',
        'merge_task_item_unittest.cpp', 'merge_task_unittest.cpp',
        'merge_work_item_creator_unittest.cpp',
        'multi_part_segment_directory_unittest.cpp',
        'multi_partition_merger_intetest.cpp',
        'multi_partition_merger_unittest.cpp',
        'pack_index_merge_meta_unittest.cpp',
        'pack_table_merge_meta_unittest.cpp',
        'package_merge_file_system_unittest.cpp',
        'parallel_end_merge_executor_unittest.cpp',
        'parallel_merge_intetest.cpp',
        'parallel_partition_data_merger_unittest.cpp',
        'segment_directory_unittest.cpp', 'table_merge_meta_unittest.cpp'
    ],
    copts=['-Werror', '-fno-access-control'],
    data=([':merger_plugin_testdata', '//aios/storage/indexlib:testdata'] +
          glob(['testdata/**'])),
    flaky=True,
    shard_count=50,
    deps=[
        ':indexlib_merger_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_table',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/config/test:indexlib_config_test_helper',
        '//aios/storage/indexlib/indexlib/index/merger_util/truncate/test:indexlib_truncate_test_helper',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper',
        '//aios/storage/indexlib/indexlib/partition/test:indexlib_partition_test_helper'
    ]
)
cc_slow_test(
    name='indexlib_merger_slow_case_unittest',
    srcs=['index_partition_merger_intetest.cpp'],
    copts=['-Werror', '-fno-access-control'],
    data=[':merger_plugin_testdata', '//aios/storage/indexlib:testdata'],
    shard_count=50,
    deps=[
        ':indexlib_merger_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_table',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/config/test:indexlib_config_test_helper',
        '//aios/storage/indexlib/indexlib/index/merger_util/truncate/test:indexlib_truncate_test_helper',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper',
        '//aios/storage/indexlib/indexlib/partition/test:indexlib_partition_test_helper'
    ]
)
cc_fast_test(
    name='exception-test',
    srcs=['merge_exception_test.cpp'],
    copts=['-Werror', '-fno-access-control'],
    deps=[
        ':indexlib_merger_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib:indexlib_testlib',
        '//aios/storage/indexlib/indexlib/config/test:indexlib_config_test_helper',
        '//aios/storage/indexlib/indexlib/index/normal/attribute/test:indexlib_attribute_test_helper',
        '//aios/storage/indexlib/indexlib/index/normal/inverted_index/builtin_index/pack/test:indexlib_pack_test_helper',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper'
    ]
)
cc_library(
    name='merger_demo_indexer_part',
    srcs=[
        'demo_index_module_factory.cpp', 'demo_index_reduce_item.cpp',
        'demo_index_segment_retriever.cpp', 'demo_indexer.cpp'
    ],
    hdrs=[
        'demo_index_module_factory.h', 'demo_index_reduce_item.h',
        'demo_index_reducer.h', 'demo_index_segment_retriever.h',
        'demo_indexer.h'
    ],
    copts=['-Werror'],
    include_prefix='indexlib/merger/test/',
    deps=['//aios/storage/indexlib/indexlib:indexlib_merger'],
    alwayslink=True
)
test_cc_plugin_shared_library(
    name='merger_demo_indexer',
    srcs=['demo_index_reducer.cpp'],
    copy_dests=['testdata/demo_indexer_plugins/'],
    include_prefix='indexlib/merger/test/',
    preloaded_deps=['//aios/storage/indexlib/indexlib:indexlib_merger'],
    static_deps=[':merger_demo_indexer_part'],
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__']
)
test_cc_plugin_shared_library(
    name='indexer_for_intetest',
    srcs=['demo_index_reducer_for_intetest.cpp'],
    copy_dests=['testdata/demo_indexer_plugins/'],
    include_prefix='indexlib/merger/test/',
    preloaded_deps=['//aios/storage/indexlib/indexlib:indexlib_merger'],
    static_deps=[':merger_demo_indexer_part'],
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__']
)
filegroup(
    name='merger_plugin_testdata',
    testonly=True,
    srcs=['indexer_for_intetest_testdata', 'merger_demo_indexer_testdata']
)
