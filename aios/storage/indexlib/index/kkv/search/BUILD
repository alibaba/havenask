load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=[':__subpackages__'])
indexlib_cc_library(
    name='search',
    srcs=[],
    hdrs=[
        'KKVCacheItem.h', 'KKVSearchCoroutine.h', 'NormalKKVIteratorImpl.h',
        'SearchCacheContext.h'
    ],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':KKVMetricsRecorder', ':SearchContext', ':search_iterator',
        '//aios/storage/indexlib/index/kkv/common:KKVSegmentIteratorBase',
        '//aios/storage/indexlib/util/cache'
    ]
)
indexlib_cc_library(
    name='kkv_index_reader',
    srcs=['KKVIndexReader.cpp'],
    hdrs=['KKVIndexReader.h'],
    visibility=['//aios/storage/indexlib/index/kkv:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/framework:SegmentMeta',
        '//aios/storage/indexlib/framework:TabletData',
        '//aios/storage/indexlib/index:IIndexReader'
    ]
)
indexlib_cc_library(
    name='search_iterator',
    srcs=['KKVIterator.cpp'],
    hdrs=['KKVIterator.h', 'KKVIteratorImplBase.h'],
    visibility=['//visibility:public'],
    deps=[
        '//aios/autil:log', '//aios/autil:time',
        '//aios/storage/indexlib/base:NoExceptionWrapper',
        '//aios/storage/indexlib/index/common/field_format/pack_attribute',
        '//aios/storage/indexlib/index/kkv:Types',
        '//aios/storage/indexlib/index/kkv/building:KKVBuildingSegmentReader',
        '//aios/storage/indexlib/index/kkv/built:kkv_disk_iterator',
        '//aios/storage/indexlib/index/kkv/built:kkv_disk_reader',
        '//aios/storage/indexlib/index/kkv/common:KKVMetricsCollector',
        '//aios/storage/indexlib/index/kkv/common:SKeySearchContext',
        '//aios/storage/indexlib/index/kkv/config'
    ]
)
indexlib_cc_library(
    name='BufferedKKVIteratorImpl',
    srcs=[],
    visibility=['//aios/storage/indexlib/index/kkv:__subpackages__'],
    deps=[':search_iterator']
)
indexlib_cc_library(
    name='SearchCacheContext',
    srcs=[],
    visibility=['//aios/storage/indexlib/index/kkv:__subpackages__'],
    deps=[
        '//aios/autil:log', '//aios/autil:time',
        '//aios/storage/indexlib/framework:SegmentMeta',
        '//aios/storage/indexlib/index/kkv/common:KKVMetricsCollector',
        '//aios/storage/indexlib/index/kkv/common:SKeySearchContext',
        '//aios/storage/indexlib/index/kkv/config',
        '//aios/storage/indexlib/util/cache'
    ]
)
indexlib_cc_library(
    name='SearchContext',
    srcs=[],
    visibility=['//aios/storage/indexlib/index/kkv:__subpackages__'],
    deps=[
        ':BufferedKKVIteratorImpl', ':SearchCacheContext', '//aios/autil:log',
        '//aios/autil:time', '//aios/storage/indexlib/framework:SegmentMeta',
        '//aios/storage/indexlib/index/kkv/common:KKVMetricsCollector',
        '//aios/storage/indexlib/index/kkv/common:SKeySearchContext',
        '//aios/storage/indexlib/index/kkv/config',
        '//aios/storage/indexlib/util/cache'
    ]
)
indexlib_cc_library(
    name='KKVMetricsRecorder',
    visibility=['//aios/storage/indexlib/index/kkv:__subpackages__'],
    deps=['//aios/kmonitor:kmonitor_cpp_adapter']
)
