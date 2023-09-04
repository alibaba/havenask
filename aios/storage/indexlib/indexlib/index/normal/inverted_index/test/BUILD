load('//bazel:defs.bzl', 'cc_fast_test')
load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_inverted_index_test_helper',
    hdrs=glob(['*h']),
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/inverted_index/test'
)
cc_fast_test(
    name='section-writer-test',
    srcs=['section_attribute_writer_unittest.cpp'],
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    shard_count=50,
    deps=[
        ':indexlib_inverted_index_test_helper', ':test-lib',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib:indexlib_testlib',
        '//aios/storage/indexlib/indexlib/index/normal/attribute/test:indexlib_attribute_test_helper',
        '//aios/storage/indexlib/indexlib/index/normal/inverted_index/builtin_index/pack/test:indexlib_pack_test_helper',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper'
    ]
)
cc_fast_test(
    name='inverted_index_unittest',
    srcs=[
        'in_mem_normal_index_segment_reader_unittest.cpp',
        'in_memory_index_segment_writer_unittest.cpp',
        'index_accessory_reader_unittest.cpp',
        'index_format_option_unittest.cpp', 'index_merger_unittest.cpp',
        'index_term_extender_unittest.cpp',
        'multi_field_index_reader_unittest.cpp',
        'multi_sharding_index_reader_unittest.cpp',
        'normal_in_doc_position_iterator_unittest.cpp',
        'normal_index_reader_unittest.cpp',
        'normal_index_segment_reader_unittest.cpp',
        'normal_index_writer_unittest.cpp',
        'patch_index_segment_updater_unittest.cpp',
        'posting_merger_impl_unittest.cpp',
        'segment_term_info_queue_unittest.cpp',
        'single_field_index_patch_iterator_unittest.cpp'
    ],
    copts=['-Werror', '-fno-access-control'],
    data=(['//aios/storage/indexlib:testdata'] + glob(['testdata/**'])),
    shard_count=50,
    deps=[
        ':indexlib_inverted_index_test_helper', ':test-lib',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/config/test:indexlib_config_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testlib',
        '//aios/storage/indexlib/indexlib/index/normal/attribute/test:indexlib_attribute_test_helper',
        '//aios/storage/indexlib/indexlib/index/normal/inverted_index/builtin_index/pack/test:indexlib_pack_test_helper',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper',
        '//aios/storage/indexlib/indexlib/util/test:indexlib_util_test_helper',
        '//aios/storage/indexlib/index/inverted_index:DocidRangePostingExecutor',
        '//aios/storage/indexlib/index/inverted_index:OrPostingExecutor'
    ]
)
cc_library(
    name='test-lib',
    testonly=True,
    srcs=[
        'index_term_extender_mock.cpp', 'normal_index_reader_helper.cpp',
        'segment_term_info_queue_mock.cpp'
    ],
    visibility=['//aios/storage/indexlib/index/inverted_index/test:__pkg__'],
    copts=['-Werror'],
    deps=[
        ':indexlib_inverted_index_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index/normal/attribute/test:indexlib_attribute_test_helper',
        '//aios/storage/indexlib/indexlib/index/normal/inverted_index/builtin_index/pack/test:indexlib_pack_test_helper',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper',
        '//aios/storage/indexlib/index/inverted_index:PostingWriterImpl',
        '//aios/storage/indexlib/index/inverted_index/test:IndexIteratorMock',
        '//aios/storage/indexlib/index/inverted_index/patch:IndexUpdateTermIterator',
        '//aios/storage/indexlib/index/inverted_index:IndexTermExtender'
    ]
)
cc_fast_test(
    name='inverted_index_intetest',
    srcs=['index_merger_intetest.cpp'],
    copts=['-Werror'],
    data=['//aios/storage/indexlib:testdata'],
    shard_count=10,
    deps=[
        ':indexlib_inverted_index_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index/normal/attribute/test:indexlib_attribute_test_helper',
        '//aios/storage/indexlib/indexlib/index/normal/inverted_index/builtin_index/pack/test:indexlib_pack_test_helper',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper',
        '//aios/storage/indexlib/index/inverted_index/test:IndexIteratorMock'
    ]
)
