load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='fslib_unittest',
    srcs=['FslibFileWrapperTest.cpp', 'FslibWrapperTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        '//aios/storage/indexlib/file_system/fslib',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
strict_cc_fast_test(
    name='DataFlushControllerTest',
    srcs=['DataFlushControllerTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        '//aios/storage/indexlib/file_system/fslib',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
