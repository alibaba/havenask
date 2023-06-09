load('@bazel_skylib//lib:selects.bzl', 'selects')
load('@bazel_skylib//rules:common_settings.bzl', 'bool_flag')
load('//bazel:defs.bzl', 'cc_proto')
load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
load(
    '//aios/storage/indexlib/index/ann/aitheta2:defs.bzl', 'aitheta_cc_library'
)
package(
    default_visibility=['//aios/storage/indexlib/index/ann:__subpackages__']
)
selects.config_setting_group(name='ppu_mode', match_all=['//:ppu_mode'])
selects.config_setting_group(
    name='gpu_mode', match_all=['//:old_abi_mode', '//:gpu_mode']
)
selects.config_setting_group(
    name='new_abi_gpu_mode', match_all=['//:new_abi_mode', '//:gpu_mode']
)
selects.config_setting_group(
    name='cpu_mode', match_all=['//:old_abi_mode', '//:cpu_mode']
)
selects.config_setting_group(
    name='new_abi_cpu_mode', match_all=['//:new_abi_mode', '//:cpu_mode']
)
cc_proto(
    name='aitheta_query_proto',
    srcs=['proto/AithetaQuery.proto'],
    include=None,
    import_prefix='indexlibv2',
    strip_import_prefix='',
    deps=[]
)
indexlib_cc_library(name='AithetaAuxSearchInfoBase', srcs=[], deps=[])
indexlib_cc_library(
    name='AithetaAuxSearchInfo',
    srcs=[],
    visibility=['//visibility:public'],
    deps=[':AithetaAuxSearchInfoBase']
)
indexlib_cc_library(
    name='AithetaQueryWrapper',
    visibility=['//visibility:public'],
    deps=[
        ':aitheta_query_proto_cc_proto', '//aios/autil:json', '//aios/autil:log'
    ]
)
indexlib_cc_library(
    name='AithetaTerm',
    srcs=[],
    visibility=['//visibility:public'],
    deps=[
        ':AithetaAuxSearchInfoBase', '//aios/storage/indexlib/index/common:Term'
    ]
)
indexlib_cc_library(
    name='aitheta2_disk_indexer',
    srcs=['AithetaDiskIndexer.cpp'],
    hdrs=['AithetaDiskIndexer.h'],
    deps=[
        '//aios/storage/indexlib/index:IndexerParameter',
        '//aios/storage/indexlib/index/ann:config',
        '//aios/storage/indexlib/index/ann/aitheta2/impl:aitheta2_segment_searcher',
        '//aios/storage/indexlib/index/ann/aitheta2/impl:customized_aitheta_logger',
        '//aios/storage/indexlib/index/ann/aitheta2/impl:normal_segment'
    ]
)
indexlib_cc_library(
    name='aitheta2_index_common',
    srcs=['AithetaIndexConfig.cpp'],
    hdrs=['AithetaIndexConfig.h', 'AithetaParams.h', 'CommonDefine.h'],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/autil:diagnostic', '//aios/autil:log',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/index/ann/aitheta2/util:param_util',
        '//aios/storage/indexlib/util:key_value_map',
        '//third_party/proxima2:proxima2_headers'
    ]
)
aitheta_cc_library(
    name='aitheta2_mem_indexer',
    srcs=['AithetaMemIndexer.cpp'],
    hdrs=['AithetaMemIndexer.h'],
    deps=[
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/document/extractor/plain:DocumentInfoExtractorFactory',
        '//aios/storage/indexlib/index:IndexerParameter',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/ann:config',
        '//aios/storage/indexlib/index/ann/aitheta2/impl:aitheta2_segment_builder',
        '//aios/storage/indexlib/index/ann/aitheta2/impl:aitheta2_segment_searcher',
        '//aios/storage/indexlib/index/ann/aitheta2/impl:customized_aitheta_logger',
        '//aios/storage/indexlib/index/ann/aitheta2/util:index_field_parser',
        '//aios/storage/indexlib/index/inverted_index:InvertedIndexReaderImpl'
    ]
)
aitheta_cc_library(
    name='aitheta2_index_merger',
    srcs=['AithetaIndexMerger.cpp'],
    hdrs=['AithetaIndexMerger.h'],
    deps=[
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/framework:Segment',
        '//aios/storage/indexlib/framework:SegmentMeta',
        '//aios/storage/indexlib/framework/index_task:IndexTaskResourceManager',
        '//aios/storage/indexlib/index:DocMapper',
        '//aios/storage/indexlib/index:IIndexMerger',
        '//aios/storage/indexlib/index:interface',
        '//aios/storage/indexlib/index/ann:config',
        '//aios/storage/indexlib/index/ann:constants',
        '//aios/storage/indexlib/index/ann/aitheta2/impl:aitheta2_segment_merger',
        '//aios/storage/indexlib/index/ann/aitheta2/impl:customized_aitheta_logger',
        '//aios/storage/indexlib/index/ann/aitheta2/impl:doc_mapper_wraper',
        '//aios/storage/indexlib/index/ann/aitheta2/util:metric_reporter'
    ]
)
indexlib_cc_library(
    name='aitheta2_index_reader',
    srcs=['AithetaIndexReader.cpp', 'AithetaRecallReporter.cpp'],
    hdrs=['AithetaIndexReader.h', 'AithetaRecallReporter.h'],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':aitheta2_disk_indexer', ':aitheta2_mem_indexer',
        '//aios/storage/indexlib/config:IIndexConfig',
        '//aios/storage/indexlib/index:IIndexReader',
        '//aios/storage/indexlib/index/ann:ANNPostingIterator',
        '//aios/storage/indexlib/index/ann/aitheta2/impl:customized_aitheta_logger'
    ]
)
indexlib_cc_library(name='SingleAithetaBuilder', deps=[':aitheta2_mem_indexer'])
indexlib_cc_library(
    name='AithetaBuildWorkItem',
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':SingleAithetaBuilder',
        '//aios/storage/indexlib/index/common:BuildWorkItem'
    ]
)
indexlib_cc_library(
    name='AithetaFilterCreator',
    srcs=[],
    deps=[
        ':AithetaAuxSearchInfo',
        '//aios/storage/indexlib/index/deletionmap:reader'
    ]
)
