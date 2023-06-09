load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
load('//bazel:defs.bzl', 'if_clang')
package(default_visibility=[':__subpackages__'])
indexlib_cc_library(
    name='factory',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':OrcIndexFactory']
)
indexlib_cc_library(
    name='reader',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':RowGroup', ':orc_index_util', ':orc_reader']
)
indexlib_cc_library(
    name='config',
    srcs=[],
    hdrs=[],
    visibility=['//visibility:public'],
    deps=[':Common', ':OrcConfig']
)
indexlib_cc_library(
    name='constants',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':Common']
)
indexlib_cc_library(
    name='OrcIndexFactory',
    deps=[
        ':config', ':orc_built_index', ':orc_logger', ':orc_mem_indexer',
        ':orc_merger', ':orc_reader', '//aios/autil:log',
        '//aios/storage/indexlib/index:IIndexFactory'
    ],
    alwayslink=True
)
indexlib_cc_library(
    name='OrcConfig',
    deps=[
        ':Common', ':OrcGeneralOptions', ':OrcReaderOptions',
        ':OrcRowReaderOptions', ':OrcWriterOptions', '//aios/autil:json',
        '//aios/autil:log', '//aios/storage/indexlib/config:FieldConfig',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/config:IndexConfigDeserializeResource',
        '@aliorc'
    ]
)
indexlib_cc_library(name='Common', srcs=[])
indexlib_cc_library(
    name='OrcPoolAdapter', deps=['//aios/autil:mem_pool_base', '@aliorc']
)
indexlib_cc_library(
    name='orc_logger',
    srcs=['AlogOrcLoggerAdapter.cpp'],
    hdrs=['AlogOrcLoggerAdapter.h'],
    deps=['//aios/alog', '@aliorc'],
    alwayslink=True
)
indexlib_cc_library(name='RowGroup', deps=['@aliorc'])
indexlib_cc_library(name='OrcGeneralOptions', deps=['//aios/autil:json'])
indexlib_cc_library(
    name='OrcWriterOptions',
    deps=[
        '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/util:Exception', '@aliorc'
    ]
)
indexlib_cc_library(
    name='OrcReaderOptions',
    deps=['//aios/autil:json', '//aios/autil:log', '@aliorc']
)
indexlib_cc_library(
    name='OrcRowReaderOptions',
    deps=[
        '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/util:Exception', '@aliorc'
    ]
)
indexlib_cc_library(
    name='orc_reader_interface',
    srcs=['IOrcReader.cpp'],
    hdrs=['IOrcIterator.h', 'IOrcReader.h'],
    copts=if_clang(['-std=c++20']),
    deps=([
        '//aios/autil:string_helper', '//aios/storage/indexlib/base:Types',
        ':RowGroup', '//aios/storage/indexlib/base:Status'
    ] + if_clang([
        '//aios/future_lite:future_lite', '//aios/future_lite:future_lite_coro'
    ]))
)
indexlib_cc_library(
    name='orc_built_index',
    srcs=([
        'OrcDiskIndexer.cpp', 'OrcLeafReader.cpp', 'OrcLeafIterator.cpp',
        'OrcIterator.cpp', 'InputStreamImpl.cpp'
    ] + if_clang(['AsyncInputStream.cpp'])),
    hdrs=([
        'OrcDiskIndexer.h', 'OrcLeafReader.h', 'OrcLeafIterator.h',
        'OrcIterator.h', 'InputStreamImpl.h'
    ] + if_clang(['AsyncInputStream.h'])),
    copts=if_clang(['-std=c++20']),
    deps=([
        ':OrcPoolAdapter', ':RowGroup', ':orc_reader_interface', ':OrcConfig',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/file_system:file_system',
        '//aios/autil:NoCopyable',
        '//aios/storage/indexlib/document:DocumentBatch',
        '//aios/storage/indexlib/document/extractor:IDocumentInfoExtractorFactory'
    ] + if_clang([
        '//aios/future_lite:future_lite', '//aios/future_lite:future_lite_coro'
    ]))
)
indexlib_cc_library(
    name='orc_index_util',
    srcs=['TypeUtils.cpp'],
    hdrs=['OrcColumnVectorBatchIterator.h', 'TypeTraits.h', 'TypeUtils.h'],
    deps=[':OrcConfig', '@aliorc']
)
indexlib_cc_library(
    name='orc_mem_indexer',
    srcs=[
        'ColumnBuffer.cpp', 'MultiStringColumnBuffer.cpp', 'OrcMemBuffer.cpp',
        'OrcMemBufferContainer.cpp', 'OrcMemIndexer.cpp'
    ],
    hdrs=[
        'ColumnBuffer.h', 'MemoryOutputStream.h', 'MultiStringColumnBuffer.h',
        'MultiValueColumnBuffer.h', 'OrcMemBuffer.h', 'OrcMemBufferContainer.h',
        'OrcMemIndexer.h', 'SingleValueColumnBuffer.h', 'StringColumnBuffer.h'
    ],
    deps=[
        ':OrcConfig', ':orc_index_util', ':orc_reader_interface',
        '//aios/autil:NoCopyable', '//aios/autil:log',
        '//aios/autil:mem_pool_container', '//aios/autil:thread',
        '//aios/storage/indexlib/base:FieldTypeUtil',
        '//aios/storage/indexlib/document:DocumentBatch',
        '//aios/storage/indexlib/document/extractor:IDocumentInfoExtractorFactory',
        '//aios/storage/indexlib/document/normal:NormalDocument',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/file_system:byte_slice_rw',
        '//aios/storage/indexlib/framework:Locator',
        '//aios/storage/indexlib/framework:SegmentMetrics',
        '//aios/storage/indexlib/index:BuildingIndexMemoryUseUpdater',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/common/field_format/attribute:attribute_convertor'
    ]
)
indexlib_cc_library(
    name='orc_reader',
    srcs=['OrcIteratorBuilder.cpp', 'OrcReader.cpp'],
    hdrs=['OrcIteratorBuilder.h', 'OrcReader.h'],
    deps=[
        ':OrcPoolAdapter', ':orc_built_index', ':orc_index_util',
        ':orc_reader_interface', '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/index:IIndexReader'
    ]
)
indexlib_cc_library(
    name='orc_merger',
    srcs=['OrcMerger.cpp', 'OutputStreamImpl.cpp'],
    hdrs=['OrcMerger.h', 'OutputStreamImpl.h'],
    deps=[
        ':OrcConfig', ':RowGroup', ':orc_built_index', ':orc_index_util',
        ':orc_reader_interface',
        '//aios/storage/indexlib/framework:SegmentMeta',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResourceManager',
        '//aios/storage/indexlib/index:DocMapper',
        '//aios/storage/indexlib/index:IIndexFactory',
        '//aios/storage/indexlib/index:IIndexMerger', '@aliorc'
    ]
)
