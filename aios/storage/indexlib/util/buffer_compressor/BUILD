load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
indexlib_cc_library(
    name='buffer_compressor',
    srcs=[
        'BufferCompressor.cpp', 'BufferCompressorCreator.cpp',
        'CompressHintDataTrainer.cpp', 'Lz4Compressor.cpp',
        'Lz4HcCompressor.cpp', 'SnappyCompressor.cpp', 'ZlibCompressor.cpp',
        'ZlibDefaultCompressor.cpp', 'ZstdCompressHintData.cpp',
        'ZstdCompressor.cpp', 'ZstdHintDataTrainer.cpp'
    ],
    hdrs=[
        'BufferCompressor.h', 'BufferCompressorCreator.h', 'CompressHintData.h',
        'CompressHintDataTrainer.h', 'Lz4CompressHintData.h', 'Lz4Compressor.h',
        'Lz4HcCompressor.h', 'SnappyCompressor.h', 'ZlibCompressor.h',
        'ZlibDefaultCompressor.h', 'ZstdCompressHintData.h', 'ZstdCompressor.h',
        'ZstdHintDataTrainer.h'
    ],
    deps=[
        '//aios/autil:mem_pool_base', '//aios/autil:zlib',
        '//aios/storage/indexlib/util:Exception',
        '//aios/storage/indexlib/util:PoolUtil',
        '//aios/storage/indexlib/util:key_value_map', '//third_party/lz4',
        '//third_party/zstd', '@snappy'
    ]
)
