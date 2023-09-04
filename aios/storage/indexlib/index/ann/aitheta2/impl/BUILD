package(
    default_visibility=['//aios/storage/indexlib/index/ann:__subpackages__']
)
load('//aios/storage:defs.bzl', 'strict_cc_library')
strict_cc_library(
    name='segment_base',
    srcs=['Segment.cpp', 'SegmentMeta.cpp'],
    hdrs=['Segment.h', 'SegmentMeta.h'],
    deps=[
        '//aios/autil:json',
        '//aios/storage/indexlib/index/ann/aitheta2:aitheta2_index_common',
        '//aios/storage/indexlib/index/ann/aitheta2/util/segment_data:parallel_segment_data_reader',
        '//aios/storage/indexlib/index/ann/aitheta2/util/segment_data:segment_data_reader',
        '//aios/storage/indexlib/index/ann/aitheta2/util/segment_data:segment_data_writer'
    ]
)
strict_cc_library(
    name='doc_map_wraper_base', srcs=[], hdrs=['DocMapWrapperBase.h'], deps=[]
)
strict_cc_library(
    name='doc_mapper_wraper',
    srcs=[],
    hdrs=['DocMapperWrapper.h'],
    deps=[':doc_map_wraper_base', '//aios/storage/indexlib/index:DocMapper']
)
strict_cc_library(
    name='normal_segment',
    srcs=['NormalSegment.cpp'],
    hdrs=['NormalSegment.h'],
    deps=[
        ':doc_map_wraper_base', ':segment_base',
        '//aios/storage/indexlib/index/ann/aitheta2:aitheta2_index_common'
    ]
)
strict_cc_library(
    name='realtime_segment',
    srcs=['RealtimeSegment.cpp'],
    hdrs=['RealtimeSegment.h'],
    deps=[
        ':realtime_build_resource',
        '//aios/storage/indexlib/index/ann/aitheta2:aitheta2_index_common',
        '//aios/storage/indexlib/index/ann/aitheta2/util:aitheta_factory_wrapper'
    ]
)
strict_cc_library(
    name='realtime_build_resource',
    srcs=['RealtimeSegmentBuildResource.cpp'],
    hdrs=['RealtimeSegmentBuildResource.h', 'SegmentBuildResource.h'],
    deps=[
        ':normal_segment',
        '//aios/storage/indexlib/index/ann/aitheta2:aitheta2_index_common'
    ]
)
strict_cc_library(
    name='aitheta2_index_searcher',
    srcs=[
        'IndexSearcher.cpp', 'NormalIndexSearcher.cpp',
        'RealtimeIndexSearcher.cpp'
    ],
    hdrs=[
        'IndexSearcher.h', 'NormalIndexSearcher.h', 'RealtimeIndexSearcher.h'
    ],
    deps=[
        ':normal_segment',
        '//aios/storage/indexlib/index/ann/aitheta2:AithetaAuxSearchInfo',
        '//aios/storage/indexlib/index/ann/aitheta2:AithetaFilterCreator',
        '//aios/storage/indexlib/index/ann/aitheta2:aitheta2_index_common',
        '//aios/storage/indexlib/index/ann/aitheta2/util:embedding_util',
        '//aios/storage/indexlib/index/ann/aitheta2/util:index_context_holder',
        '//aios/storage/indexlib/index/ann/aitheta2/util:metric_reporter',
        '//aios/storage/indexlib/index/ann/aitheta2/util:query_parser',
        '//aios/storage/indexlib/index/ann/aitheta2/util:result_holder',
        '//aios/storage/indexlib/index/deletionmap:reader'
    ]
)
strict_cc_library(
    name='aitheta2_segment_searcher',
    srcs=[
        'NormalSegmentSearcher.cpp', 'RealtimeSegmentSearcher.cpp',
        'SegmentSearcher.cpp'
    ],
    hdrs=[
        'NormalSegmentSearcher.h', 'RealtimeSegmentSearcher.h',
        'SegmentSearcher.h'
    ],
    deps=[
        ':aitheta2_index_searcher', ':normal_segment', ':realtime_segment',
        '//aios/storage/indexlib/index/ann/aitheta2:aitheta_query_proto_cc_proto'
    ]
)
strict_cc_library(
    name='aitheta2_segment_builder',
    srcs=[
        'NormalIndexBuilder.cpp', 'NormalSegmentBuilder.cpp',
        'RealtimeIndexBuilder.cpp', 'RealtimeSegmentBuilder.cpp',
        'SegmentBuilder.cpp'
    ],
    hdrs=[
        'NormalIndexBuilder.h', 'NormalSegmentBuilder.h',
        'RealtimeIndexBuilder.h', 'RealtimeSegmentBuilder.h',
        'SegmentBuildResource.h', 'SegmentBuilder.h'
    ],
    deps=[
        ':realtime_segment', ':segment_base',
        '//aios/storage/indexlib/index/ann/aitheta2:aitheta2_index_common',
        '//aios/storage/indexlib/index/ann/aitheta2/util:embedding_field_data',
        '//aios/storage/indexlib/index/ann/aitheta2/util:embedding_util',
        '//aios/storage/indexlib/index/ann/aitheta2/util:index_context_holder',
        '//aios/storage/indexlib/index/ann/aitheta2/util:metric_reporter',
        '//aios/storage/indexlib/index/ann/aitheta2/util:pk_data_util'
    ]
)
strict_cc_library(
    name='aitheta2_segment_merger',
    srcs=['NormalSegmentMerger.cpp'],
    hdrs=['NormalSegmentMerger.h'],
    deps=[':aitheta2_segment_builder']
)
strict_cc_library(
    name='customized_aitheta_logger',
    srcs=['CustomizedAithetaLogger.cpp'],
    hdrs=['CustomizedAithetaLogger.h'],
    deps=[
        '//aios/autil:diagnostic', '//aios/autil:lock', '//aios/autil:log',
        '@proxima2//:proxima2_headers'
    ]
)
