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
        'ByteSliceWriterTest.cpp', 'DirectoryTest.cpp', 'DiskStorageTest.cpp',
        'EntryTableTest.cpp', 'FenceDirectoryTest.cpp',
        'FileBlockCacheContainerTest.cpp', 'FileBlockCacheTest.cpp',
        'FileSystemFileTest.cpp', 'FileSystemIntetest.cpp',
        'FileSystemListFileTest.cpp', 'FileSystemMetricsReporterTest.cpp',
        'FileSystemRemoveTest.cpp', 'FileSystemStorageTest.cpp',
        'IFileSystemTest.cpp', 'IndexFileDeployerTest.cpp',
        'LifecycleConfigTest.cpp', 'LifecycleTableTest.cpp',
        'LoadConfigListTest.cpp', 'LocalDirectoryTest.cpp',
        'LogicalFileSystemIntetest.cpp', 'MemDirectoryTest.cpp',
        'MemStorageTest.cpp', 'ResourceFileTest.cpp', 'StorageMetricsTest.cpp'
    ],
    copts=['-fno-access-control'],
    data=glob(['testdata/*']),
    shard_count=5,
    deps=[
        ':test-util', '//aios/storage/indexlib/file_system:byte_slice_rw',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
strict_cc_fast_test(
    name='file_system_dadi_cache_unittest',
    srcs=['FileBlockCacheWithDadiTest.cpp'],
    copts=['-fno-access-control'],
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
