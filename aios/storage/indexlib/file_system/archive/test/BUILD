load('//aios/storage:defs.bzl', 'strict_cc_fast_test', 'strict_cc_slow_test')
strict_cc_fast_test(
    name='archive_unittest',
    srcs=[
        'ArchiveDirectoryTest.cpp', 'ArchiveFileTest.cpp',
        'ArchiveFolderTest.cpp', 'LineReaderTest.cpp'
    ],
    copts=['-fno-access-control'],
    deps=[
        '//aios/autil:string_helper', '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
strict_cc_slow_test(
    name='archive_exception_unittest',
    srcs=['ArchiveFolderExceptionTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/util/testutil:exception_runner',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
