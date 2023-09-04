load(
    '//aios/storage:defs.bzl', 'strict_cc_fast_test', 'strict_cc_library',
    'strict_cc_slow_test'
)
strict_cc_library(
    name='test-util',
    testonly=True,
    srcs=[
        'FileSystemTestUtil.cpp', 'LoadConfigListCreator.cpp',
        'TestFileCreator.cpp'
    ],
    hdrs=[
        'FakeFileNode.h', 'FileSystemTestUtil.h', 'LoadConfigListCreator.h',
        'PackageFileUtil.h', 'TestFileCreator.h'
    ],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=(['//aios/storage/indexlib/file_system'] +
          ['//aios/future_lite/future_lite/executors:simple_async_io_executor'])
)
strict_cc_library(
    name='directory-checker',
    testonly=True,
    srcs=['DirectoryChecker.cpp'],
    hdrs=['DirectoryChecker.h'],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/autil:log', '//aios/storage/indexlib/base:PathUtil',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/util:path_util'
    ]
)
strict_cc_fast_test(
    name='file_system_unittest',
    srcs=[
        'IndexFileDeployerTest.cpp', 'ByteSliceWriterTest.cpp',
        'DirectoryTest.cpp', 'FenceDirectoryTest.cpp', 'DiskStorageTest.cpp',
        'EntryTableTest.cpp', 'FileBlockCacheTest.cpp',
        'FileBlockCacheContainerTest.cpp', 'FileSystemFileTest.cpp',
        'FileSystemListFileTest.cpp', 'FileSystemRemoveTest.cpp',
        'FileSystemStorageTest.cpp', 'MemDirectoryTest.cpp',
        'IFileSystemTest.cpp', 'LifecycleTableTest.cpp',
        'LifecycleConfigTest.cpp', 'LoadConfigListTest.cpp',
        'LocalDirectoryTest.cpp', 'MemStorageTest.cpp',
        'StorageMetricsTest.cpp', 'ResourceFileTest.cpp',
        'FileSystemIntetest.cpp', 'LogicalFileSystemIntetest.cpp',
        'FileSystemMetricsReporterTest.cpp'
    ],
    copts=['-fno-access-control'],
    data=glob(['testdata/*']),
    shard_count=20,
    deps=[
        ':test-util', '//aios/storage/indexlib/file_system:byte_slice_rw',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
strict_cc_fast_test(
    name='file_system_dadi_cache_unittest',
    srcs=['FileBlockCacheWithDadiTest.cpp'],
    data=glob(['testdata/*']),
    deps=[':test-util', '//aios/storage/indexlib/util/testutil:unittest']
)
strict_cc_slow_test(
    name='file_system_perf_unittest',
    srcs=['IFileSystemPerftest.cpp', 'MunmapPerftest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        ':test-util', '//aios/storage/indexlib/file_system:byte_slice_rw',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
strict_cc_fast_test(
    name='exception_unittest',
    srcs=['MemStorageExceptionTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        ':test-util',
        '//aios/storage/indexlib/file_system/test:directory-checker',
        '//aios/storage/indexlib/util/testutil:exception_runner',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
