load(
    '//aios/storage:defs.bzl', 'strict_cc_fast_test', 'strict_cc_library',
    'strict_cc_slow_test'
)
strict_cc_library(
    name='indexlib_file_system_file_test_helper',
    srcs=[],
    hdrs=['MockFslibFile.h']
)
strict_cc_fast_test(
    name='file_unittest',
    srcs=[
        'BlockByteSliceListTest.cpp', 'BlockFileNodeCreatorTest.cpp',
        'BlockFileNodeTest.cpp', 'BlockPrefetcherTest.cpp',
        'BufferedFileNodeTest.cpp', 'BufferedFileOutputStreamTest.cpp',
        'BufferedFileReaderTest.cpp', 'BufferedFileWriterTest.cpp',
        'CompressBlockDataRetrieverTest.cpp',
        'CompressFileAddressMapperEncoderTest.cpp',
        'CompressFileAddressMapperTest.cpp', 'CompressFileReaderTest.cpp',
        'CompressFileWriterTest.cpp', 'FileNodeCacheTest.cpp',
        'FileNodeCreatorTest.cpp', 'FileReaderTest.cpp', 'InMemFileTest.cpp',
        'InterimFileWriterTest.cpp', 'MemFileNodeTest.cpp',
        'MemFileWriterTest.cpp', 'MmapFileNodeTest.cpp',
        'NoCompressBlockDataRetrieverTest.cpp', 'SessionFileCacheTest.cpp',
        'SliceFileNodeTest.cpp', 'SliceFileTest.cpp', 'SwapMmapFileTest.cpp'
    ],
    copts=['-fno-access-control'],
    data=glob(['testdata/*']),
    tags=['disabled'],
    deps=[
        ':indexlib_file_system_file_test_helper',
        '//aios/future_lite/future_lite/executors:simple_executor',
        '//aios/storage/indexlib/file_system:byte_slice_rw',
        '//aios/storage/indexlib/file_system/test:test-util',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
strict_cc_slow_test(
    name='file_slow_unittest',
    srcs=glob(['BlockFileNodePerfTest.cpp'], exclude=[]),
    copts=['-fno-access-control'],
    deps=[
        ':indexlib_file_system_file_test_helper',
        '//aios/storage/indexlib/file_system:byte_slice_rw',
        '//aios/storage/indexlib/file_system/test:test-util',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
