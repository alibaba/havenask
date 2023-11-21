load('//bazel:defs.bzl', 'cc_fast_test')
cc_library(
    name='indexlib_util_test_helper',
    testonly=True,
    hdrs=glob(['*.h']),
    copts=['-Werror'],
    include_prefix='indexlib/util/test',
    visibility=[
        '//aios/storage/indexlib/indexlib:__subpackages__',
        '//aios/storage/indexlib/indexlib/tools:__subpackages__'
    ]
)
cc_fast_test(
    name='indexlib_util_unittest',
    srcs=[
        'class_typed_factory_unittest.cpp',
        'fixed_size_object_slab_unittest.cpp', 'priority_queue_unittest.cpp',
        'resource_control_thread_pool_unittest.cpp', 'unittest_unittest.cpp'
    ],
    copts=['-Werror', '-fno-access-control'],
    data=(['//aios/storage/indexlib:testdata'] + glob(['testdata/*'])),
    shard_count=5,
    deps=[
        ':indexlib_util_test_helper',
        '//aios/storage/indexlib/indexlib:indexlib_testbase',
        '//aios/storage/indexlib/indexlib/util:util_all',
        '//aios/storage/indexlib/util:PoolUtil'
    ]
)
