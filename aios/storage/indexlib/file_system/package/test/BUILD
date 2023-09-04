load('//aios/storage:defs.bzl', 'strict_cc_fast_test', 'strict_cc_library')
strict_cc_library(
    name='PackageTestUtil',
    deps=['//aios/storage/indexlib/file_system/package']
)
strict_cc_fast_test(
    name='package_unittest',
    srcs=[
        'DirectoryMergerTest.cpp', 'PackageDiskStorageTest.cpp',
        'PackageMemStorageTest.cpp', 'PackageFileMetaTest.cpp',
        'VersionedPackageFileMetaTest.cpp'
    ],
    copts=['-fno-access-control'],
    deps=[
        ':PackageTestUtil',
        '//aios/storage/indexlib/file_system/test:test-util',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
strict_cc_fast_test(
    name='PackageFileTagConfigListTest',
    srcs=['PackageFileTagConfigListTest.cpp'],
    deps=[
        '//aios/storage/indexlib/file_system/package',
        '//aios/storage/indexlib/file_system/test:test-util',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
strict_cc_fast_test(
    name='MergePackageUtilTest',
    srcs=['MergePackageUtilTest.cpp'],
    data=glob(['testdata/**']),
    deps=[
        ':PackageTestUtil', '//aios/storage/indexlib/file_system/package',
        '//aios/storage/indexlib/file_system/test:test-util',
        '//aios/storage/indexlib/util/testutil:exception_runner',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
