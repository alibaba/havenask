load('//bazel:defs.bzl', 'cc_fast_test', 'copy_target_to')
load(
    '@rules_cc//examples:experimental_cc_shared_library.bzl',
    'cc_shared_library'
)
cc_library(
    name='indexlib_customized_index_test_helper',
    hdrs=glob(['*h']),
    copts=['-Werror'],
    include_prefix='indexlib/index/normal/inverted_index/customized_index/test'
)
filegroup(
    name='demo_indexer_cpp',
    srcs=[
        'demo_in_mem_segment_retriever.cpp', 'demo_index_module_factory.cpp',
        'demo_index_reduce_item.cpp', 'demo_index_reducer.cpp',
        'demo_index_retriever.cpp', 'demo_index_segment_retriever.cpp',
        'demo_indexer.cpp'
    ],
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__']
)
filegroup(
    name='demo_indexer_h',
    srcs=[
        'demo_in_mem_segment_retriever.h', 'demo_index_module_factory.h',
        'demo_index_reduce_item.h', 'demo_index_reducer.h',
        'demo_index_retriever.h', 'demo_index_segment_retriever.h',
        'demo_indexer.h', 'demo_user_data.h'
    ],
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__']
)
cc_library(
    name='demo_indexer_cc',
    testonly=True,
    srcs=[':demo_indexer_cpp'],
    hdrs=[':demo_indexer_h'],
    include_prefix='indexlib/index/normal/inverted_index/customized_index/test',
    deps=[
        '//aios/storage/indexlib/indexlib:indexlib_plugin',
        '//aios/storage/indexlib/indexlib/index'
    ]
)
cc_shared_library(
    name='demo_indexer.so',
    testonly=True,
    preloaded_deps=[
        '//aios/storage/indexlib/indexlib:indexlib_plugin',
        '//aios/storage/indexlib/indexlib/index:index'
    ],
    roots=['demo_indexer_cc'],
    visibility=['//aios/storage/indexlib/indexlib:__subpackages__']
)
copy_target_to(
    name='demo_indexer_testdata',
    testonly=True,
    copy_name='libdemo_indexer.so',
    dests=['testdata/demo_indexer_plugins/'],
    to_copy=':demo_indexer.so'
)
cc_fast_test(
    name='indexlib_customized_index_unittest',
    srcs=glob(['*unittest.cpp', '*intetest.cpp'],
              exclude=['faiss_indexer_unittest.cpp']),
    copts=['-Werror', '-fno-access-control'],
    data=([':demo_indexer_testdata', '//aios/storage/indexlib:test_log_conf'] +
          glob(['testdata/**'])),
    deps=[
        ':indexlib_customized_index_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/config/test:indexlib_config_test_helper',
        '//aios/storage/indexlib/indexlib/index/test:indexlib_index_test_helper',
        '//aios/storage/indexlib/indexlib/test:directory-creator',
        '//aios/storage/indexlib/indexlib/util/test:indexlib_util_test_helper'
    ]
)
