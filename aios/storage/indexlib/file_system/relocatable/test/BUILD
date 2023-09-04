load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='unittests',
    srcs=['RelocatorTest.cpp'],
    copts=['-fno-access-control'],
    data=glob(['testdata/*']),
    tags=['disabled'],
    deps=[
        '//aios/storage/indexlib/file_system/file:interface',
        '//aios/storage/indexlib/file_system/fslib',
        '//aios/storage/indexlib/file_system/relocatable:relocator',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
