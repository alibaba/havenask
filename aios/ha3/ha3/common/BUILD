package(default_visibility=['//aios/ha3:__subpackages__'])
load('//bazel:defs.bzl', 'gen_error_info')
ha3_request_source_files = [
    'AggregateClause.cpp', 'DistinctClause.cpp', 'HealthCheckClause.cpp',
    'QueryLayerClause.cpp', 'VirtualAttributeClause.cpp', 'AnalyzerClause.cpp',
    'DocIdClause.cpp', 'KVPairClause.cpp', 'RankClause.cpp',
    'AttributeClause.cpp', 'FetchSummaryClause.cpp', 'OptimizerClause.cpp',
    'RankSortClause.cpp', 'ClusterClause.cpp', 'FilterClause.cpp',
    'PKFilterClause.cpp', 'SearcherCacheClause.cpp', 'ConfigClause.cpp',
    'FinalSortClause.cpp', 'QueryClause.cpp', 'SortClause.cpp', 'Request.cpp',
    'ClauseBase.cpp', 'AggFunDescription.cpp', 'DistinctDescription.cpp',
    'RankSortDescription.cpp', 'AggregateDescription.cpp',
    'LayerDescription.cpp', 'SortDescription.cpp', 'RankHint.cpp'
]
cc_library(
    name='timeout_terminator',
    hdrs=['TimeoutTerminator.h'],
    include_prefix='ha3/common',
    deps=['//aios/autil:result', '//aios/autil:timeout_terminator']
)
cc_library(
    name='common_def',
    hdrs=['CommonDef.h'],
    include_prefix='ha3/common',
    visibility=['//visibility:public'],
    deps=['//aios/ha3:ha3_isearch_h']
)
cc_library(
    name='ha3_error_result_headers',
    hdrs=['ErrorResult.h'],
    include_prefix='ha3/common',
    deps=[':ha3_error_define_headers', '//aios/autil:log']
)
cc_library(
    name='ha3_error_result',
    srcs=['ErrorResult.cpp'],
    include_prefix='ha3/common',
    visibility=['//visibility:public'],
    deps=[
        ':ha3_error_define', ':ha3_error_result_headers',
        '//aios/autil:data_buffer', '//aios/autil:log'
    ]
)
cc_library(
    name='ha3_error_define_headers',
    hdrs=[':ha3_error_define_files'],
    include_prefix='ha3/common'
)
cc_library(
    name='ha3_error_define',
    srcs=[':ha3_error_define_files'],
    deps=[':ha3_error_define_headers']
)
gen_error_info(
    name='ha3_error_define_files',
    srcs=['ErrorDefine.ed'],
    outs=['ErrorDefine.cpp', 'ErrorDefine.h', 'ErrorDefine.py']
)
cc_test(
    name='error_result_test',
    srcs=glob(['test/ErrorResultTest.cpp']),
    copts=['-fno-access-control'],
    data=['//aios/ha3:testdata'],
    linkopts=['-Wl,--as-needed'],
    deps=[':ha3_error_result', '//aios/unittest_framework']
)
cc_test(
    name='timeout_terminator_test',
    srcs=glob(['test/TimeoutTerminatorTest.cpp']),
    copts=['-fno-access-control'],
    data=['//aios/ha3:testdata'],
    linkopts=['-Wl,--as-needed'],
    deps=[':timeout_terminator', '//aios/unittest_framework']
)
