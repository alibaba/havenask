load('//aios/storage:defs.bzl', 'strict_cc_library')
package(
    default_visibility=['//aios/storage/indexlib/file_system:__subpackages__']
)
strict_cc_library(
    name='interface',
    srcs=[],
    hdrs=['FileReader.h', 'FileWriter.h', 'ReadOption.h'],
    visibility=['//visibility:public'],
    deps=[
        '//aios/autil:NoCopyable', '//aios/future_lite',
        '//aios/storage/indexlib/util:CoroutineConfig'
    ]
)
strict_cc_library(
    name='headers',
    srcs=[],
    hdrs=[
        'BlockByteSliceList.h', 'BlockDataRetriever.h', 'BlockPrefetcher.h',
        'BufferedFileWriter.h', 'DecompressMetricsReporter.h', 'FileBuffer.h',
        'FileNode.h', 'FileWriterImpl.h'
    ],
    deps=[':interface']
)
strict_cc_library(name='file', srcs=[], hdrs=[], deps=[':base', ':compress'])
strict_cc_library(
    name='compress',
    srcs=[
        'CompressDataDumper.cpp', 'CompressFileAddressMapper.cpp',
        'CompressFileAddressMapperEncoder.cpp', 'CompressFileReader.cpp',
        'CompressFileReaderCreator.cpp', 'CompressFileWriter.cpp',
        'CompressHintDataReader.cpp', 'DecompressCachedCompressFileReader.cpp',
        'HintCompressDataDumper.cpp', 'IntegratedCompressFileReader.cpp',
        'MemFileWriter.cpp', 'NormalCompressFileReader.cpp',
        'SnappyCompressFileReader.cpp', 'SnappyCompressFileWriter.cpp',
        'SwapMmapFileWriter.cpp'
    ],
    hdrs=[
        'CompressDataDumper.h', 'CompressFileAddressMapper.h',
        'CompressFileMeta.h', 'CompressFileReader.h',
        'CompressFileReaderCreator.h', 'CompressFileWriter.h',
        'CompressHintDataReader.h', 'DecompressCachedCompressFileReader.h',
        'HintCompressDataDumper.h', 'IntegratedCompressFileReader.h',
        'MemFileWriter.h', 'NormalCompressFileReader.h',
        'SnappyCompressFileReader.h', 'SnappyCompressFileWriter.h',
        'SwapMmapFileWriter.h'
    ],
    deps=[
        ':base', '//aios/autil:NoCopyable',
        '//aios/storage/indexlib/file_system:Storage',
        '//aios/storage/indexlib/file_system:interface'
    ]
)
strict_cc_library(
    name='base',
    srcs=[
        'BlockByteSliceList.cpp', 'BlockFileAccessor.cpp', 'BlockFileNode.cpp',
        'BlockFileNodeCreator.cpp', 'BlockPrefetcher.cpp',
        'BufferedFileNode.cpp', 'BufferedFileNodeCreator.cpp',
        'BufferedFileOutputStream.cpp', 'BufferedFileReader.cpp',
        'BufferedFileWriter.cpp', 'CompressBlockDataRetriever.cpp',
        'DecompressCachedBlockDataRetriever.cpp', 'DirectoryFileNode.cpp',
        'DirectoryFileNodeCreator.cpp', 'FileBuffer.cpp', 'FileNode.cpp',
        'FileNodeCache.cpp', 'FileNodeCreator.cpp', 'FileReader.cpp',
        'FileWorkItem.cpp', 'FileWriterImpl.cpp',
        'IntegratedCompressBlockDataRetriever.cpp', 'InterimFileWriter.cpp',
        'MemFileNode.cpp', 'MemFileNodeCreator.cpp', 'MmapFileNode.cpp',
        'MmapFileNodeCreator.cpp', 'NoCompressBlockDataRetriever.cpp',
        'NormalCompressBlockDataRetriever.cpp', 'NormalFileReader.cpp',
        'ResourceFile.cpp', 'ResourceFileNode.cpp', 'SessionFileCache.cpp',
        'SliceFileNode.cpp', 'SliceFileReader.cpp', 'SliceFileWriter.cpp',
        'SwapMmapFileNode.cpp', 'SwapMmapFileReader.cpp', 'TempFileWriter.cpp'
    ],
    hdrs=[
        'BlockFileAccessor.h', 'BlockFileNode.h', 'BlockFileNodeCreator.h',
        'BufferedFileNode.h', 'BufferedFileNodeCreator.h',
        'BufferedFileOutputStream.h', 'BufferedFileReader.h',
        'CompressBlockDataRetriever.h', 'CompressFileAddressMapper.h',
        'CompressFileAddressMapperEncoder.h', 'CompressFileInfo.h',
        'CompressFileReader.h', 'CompressFileReaderCreator.h',
        'DecompressCachedBlockDataRetriever.h', 'DirectoryFileNode.h',
        'DirectoryFileNodeCreator.h', 'DirectoryMapIterator.h', 'FileCarrier.h',
        'FileNodeCache.h', 'FileNodeCreator.h', 'FileWorkItem.h',
        'IntegratedCompressBlockDataRetriever.h', 'InterimFileWriter.h',
        'MemFileNode.h', 'MemFileNodeCreator.h', 'MmapFileNode.h',
        'MmapFileNodeCreator.h', 'NoCompressBlockDataRetriever.h',
        'NormalCompressBlockDataRetriever.h', 'NormalFileReader.h',
        'ResourceFile.h', 'ResourceFileNode.h', 'SessionFileCache.h',
        'SliceFileNode.h', 'SliceFileReader.h', 'SliceFileWriter.h',
        'SwapMmapFileNode.h', 'SwapMmapFileReader.h', 'TempFileWriter.h'
    ],
    deps=[
        ':headers', ':interface', '//aios/autil:diagnostic',
        '//aios/storage/indexlib/file_system:common',
        '//aios/storage/indexlib/file_system/fslib',
        '//aios/storage/indexlib/file_system/load_config',
        '//aios/storage/indexlib/file_system/package:meta',
        '//aios/storage/indexlib/util:future_executor',
        '//aios/storage/indexlib/util:mmap_pool',
        '//aios/storage/indexlib/util:path_util',
        '//aios/storage/indexlib/util/buffer_compressor',
        '//aios/storage/indexlib/util/byte_slice_list',
        '//aios/storage/indexlib/util/cache',
        '//aios/storage/indexlib/util/memory_control',
        '//aios/storage/indexlib/util/metrics:metric_reporter',
        '//aios/storage/indexlib/util/slice_array'
    ]
)
