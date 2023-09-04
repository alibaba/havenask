load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='file_system_unittest',
    srcs=[
        'AsyncDumpSchedulerTest.cpp', 'DirOperationCacheTest.cpp',
        'FlushOperationQueueTest.cpp'
    ],
    copts=['-fno-access-control'],
    data=glob(['testdata/*']),
    deps=[
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system/flush',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
