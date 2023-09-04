load('@rules_cc//cc:defs.bzl', 'cc_library')
load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_attribute_test_helper',
    testonly=True,
    srcs=[
        'attribute_merger_test_util.cpp', 'attribute_test_util.cpp',
        'attribute_writer_helper.cpp', 'section_attribute_test_util.cpp',
        'single_value_attribute_reader_creator.cpp'
    ],
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/attribute/test',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper'
    ]
)
cc_fast_test(
    name='todo',
    srcs=[],
    copts=['-Werror'],
    data=['//aios/storage/indexlib:testdata'],
    shard_count=20,
    deps=[
        ':indexlib_attribute_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper',
        '//aios/storage/indexlib/indexlib/util:util_all'
    ]
)
cc_fast_test(
    name='attribute-simple-test',
    srcs=['pack_attr_update_doc_size_calculator_unittest.cpp'],
    copts=['-Werror', '-fno-access-control'],
    data=['//aios/storage/indexlib:testdata'],
    flaky=True,
    shard_count=50,
    deps=[
        ':indexlib_attribute_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper',
        '//aios/storage/indexlib/indexlib/util:util_all',
        '//aios/storage/indexlib/indexlib/util/test:indexlib_util_test_helper',
        '//aios/storage/indexlib/util/cache/test:fake_dadi'
    ]
)
cc_fast_test(
    name='attribute_intetest',
    srcs=[
        'attribute_patch_merge_intetest.cpp', 'shape_attribute_intetest.cpp',
        'updatable_var_num_attribute_merger_intetest.cpp'
    ],
    copts=['-Werror'],
    data=['//aios/storage/indexlib:testdata'],
    shard_count=20,
    deps=[
        ':indexlib_attribute_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper',
        '//aios/storage/indexlib/indexlib/util:util_all',
        '//aios/storage/indexlib/indexlib/util/test:indexlib_util_test_helper'
    ]
)
