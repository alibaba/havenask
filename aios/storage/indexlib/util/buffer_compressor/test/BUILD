load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='buffer_compressor_unittest',
    srcs=[
        'BufferCompressorTest.cpp', 'BufferCompressorTest.h',
        'Lz4CompressorTest.cpp', 'Lz4HcCompressorTest.cpp',
        'SnappyCompressorTest.cpp', 'ZlibCompressorTest.cpp',
        'ZlibDefaultCompressorTest.cpp', 'ZstdCompressorTest.cpp'
    ],
    copts=['-fno-access-control'],
    shard_count=2,
    deps=[
        '//aios/storage/indexlib/util/buffer_compressor',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
