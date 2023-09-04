load('@rules_cc//cc:defs.bzl', 'cc_library')
load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_index_test_helper',
    testonly=True,
    srcs=[
        'document_checker_for_gtest.cpp', 'document_maker.cpp',
        'index_document_maker.cpp', 'index_test_util.cpp',
        'mock_segment_directory.cpp', 'partition_info_creator.cpp',
        'posting_maker.cpp', 'section_data_maker.cpp'
    ],
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/index/test',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/file_system/test:test-util',
        '//aios/storage/indexlib/indexlib:indexlib_merger',
        '//aios/storage/indexlib/indexlib:indexlib_testbase_helper',
        '//aios/storage/indexlib/indexlib/index',
        '//aios/storage/indexlib/indexlib/partition:partition_reader',
        '//aios/storage/indexlib/indexlib/test:index_meta_maker',
        '//aios/storage/indexlib/indexlib/test:partition_state_machine',
        '//aios/storage/indexlib/indexlib/test:single_field_partition_data_provider',
        '//aios/storage/indexlib/indexlib/util/test:indexlib_util_test_helper'
    ]
)
cc_fast_test(
    name='indexlib_index_unittest',
    srcs=glob([
        'partition_info_unittest.cpp',
        'lifecycle_segment_metrics_updater_unittest.cpp',
        'time_series_segment_metrics_updater_unittest.cpp',
        'multi_segment_metrics_updater_unittest.cpp',
        'max_min_segment_metrics_updater_unittest.cpp',
        'var_len_data_accessor_unittest.cpp',
        'temperature_segment_metrics_updater_unittest.cpp',
        'filter_matcher_unittest.cpp'
    ]),
    copts=['-Werror'],
    data=(['//aios/storage/indexlib:testdata'] + glob(['testdata/**'])),
    deps=[
        ':indexlib_index_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/config/test:indexlib_config_test_helper'
    ]
)
