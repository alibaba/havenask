package(default_visibility=['//aios/ha3:__subpackages__'])
load('//bazel:defs.bzl', 'gen_error_info', 'cc_proto')
cc_library(
    name='ha3_common_headers',
    hdrs=glob(['*.h']),
    deps=[
        ':ha3_error_define_headers', ':ha3_pbresult_proto_cc_proto_headers',
        '//aios/network/gig/multi_call/common:common_headers',
        '//aios/ha3/ha3/common/access_log:access_log_headers',
        '//aios/ha3/ha3/common/query:ha3_query_headers',
        '//aios/ha3/ha3/common/searchinfo:ha3_searchinfo_headers',
        '@suez_turing.turbojet.turing_ops_util//:turing_ops_util'
    ],
    include_prefix='ha3/common'
)
cc_library(
    name='ha3_common_base',
    srcs=['GlobalIdentifier.cpp'],
    hdrs=['GlobalIdentifier.h', 'Tracer.h', 'RequestSymbolDefine.h'],
    include_prefix='ha3/common',
    deps=[
        '//aios/ha3:ha3_util',
        '@suez_turing.turbojet.turing_ops_util//:turing_ops_util'
    ]
)
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
    name='ha3_request',
    srcs=ha3_request_source_files,
    hdrs=(
        glob(['*Clause.h', '*Description.h']) +
        ['Request.h', 'ClauseBase.h', 'RankHint.h']
    ),
    include_prefix='ha3/common',
    deps=[
        ':ha3_common_base', '//aios/autil:range_util',
        '//aios/network/gig/multi_call/common:common_headers',
        '//aios/ha3:ha3_util', '//aios/ha3:ha3_proto',
        '//aios/ha3/ha3/common/query:ha3_query',
        '@suez_turing.turbojet.turing_ops_util//:suez_turing'
    ]
)
cc_library(
    name='ha3_common',
    srcs=glob(['*.cpp'],
              exclude=(
                  ha3_request_source_files +
                  ['GlobalIdentifier.cpp', 'ErrorResult.cpp']
              )),
    deps=[
        ':ha3_pbresult_proto_cc_proto', ':ha3_error_define',
        ':ha3_error_result', ':ha3_request', '//aios/ha3:ha3_framework_headers',
        '//aios/ha3/ha3/common/access_log:access_log',
        '//aios/ha3/ha3/common/searchinfo:ha3_searchinfo',
        '//aios/ha3/ha3/common/query:ha3_query', '@rapidjson//:rapidjson'
    ]
)
cc_library(
    name='timeout_terminator',
    hdrs=['TimeoutTerminator.h'],
    deps=['//aios/autil:timeout_terminator', '//aios/autil:result'],
    include_prefix='ha3/common'
)
cc_library(
    name='common_def',
    hdrs=['CommonDef.h'],
    deps=['//aios/ha3:ha3_isearch_h'],
    include_prefix='ha3/common'
)
cc_library(
    name='ha3_error_result_headers',
    hdrs=['ErrorResult.h'],
    deps=['//aios/autil:log', ':ha3_error_define_headers'],
    include_prefix='ha3/common'
)
cc_library(
    name='ha3_error_result',
    srcs=['ErrorResult.cpp'],
    deps=[
        ':ha3_error_result_headers', ':ha3_error_define',
        '//aios/autil:data_buffer', '//aios/autil:log'
    ],
    include_prefix='ha3/common'
)
cc_library(
    name='ha3_tracer_headers',
    hdrs=['Tracer.h'],
    deps=['@suez_turing.turbojet.turing_ops_util//:turing_ops_util'],
    include_prefix='ha3/common'
)
cc_library(
    name='ha3_tracer',
    deps=[
        ':ha3_tracer_headers',
        '@suez_turing.turbojet.turing_ops_util//:turing_ops_util'
    ]
)
cc_proto(
    name='ha3_pbresult_proto',
    srcs=['PBResult.proto'],
    deps=[],
    import_prefix='ha3/common/'
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
