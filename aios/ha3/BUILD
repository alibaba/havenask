package(default_visibility=['//aios/ha3:__subpackages__'])
load('@rules_pkg//:deps.bzl', 'rules_pkg_dependencies')
load(
    '//bazel:defs.bzl', 'gen_cpp_code', 'copy_target_to', 'cc_proto',
    'gen_conf_lib', 'genyacc', 'genlex', 'test_cc_plugin_shared_library'
)
load('@flatbuffers//:build_defs.bzl', 'flatbuffer_library_public')
load('//bazel:bundle.bzl', 'bundle_files')
cc_library(
    name='ha3_util',
    srcs=glob(['ha3/util/*.cpp']),
    hdrs=glob(['ha3/util/*.h']),
    deps=[':ha3_isearch_h', '//aios/autil:log', '//aios/autil:compression'],
    visibility=['//visibility:public'],
    include_prefix='ha3',
    strip_include_prefix='ha3'
)
cc_library(
    name='ha3_framework_headers',
    hdrs=(
        glob([
            'ha3/util/*.h', 'ha3/schema/*.h', 'ha3/config/*.h', 'ha3/index/*.h',
            'ha3/search/*.h', 'ha3/service/*.h', 'ha3/worker/*.h',
            'ha3/proxy/*.h', 'ha3/func_expression/*.h', 'ha3/sorter/*.h',
            'ha3/worker/*.h', 'ha3/turing/variant/*.h',
            'ha3/turing/ops/agg/*.h', 'ha3/turing/common/*.h',
            'ha3/turing/qrs/*.h', 'ha3/turing/searcher/*.h', 'ha3/turing/*.h',
            'ha3/cava/*.h', 'ha3/cava/scorer/*.h'
        ]) +
        glob([
            'ha3/qrs/*.h', 'ha3/summary/*.h', 'ha3/rank/*.h',
            'ha3/turing/ops/*.h'
        ],
             exclude=[
                 'ha3/qrs/AdapterQrsProcessor.h',
                 'ha3/summary/AdapterSummary.h', 'ha3/rank/AdapterScorer.h',
                 'ha3/turing/ops/Ha3CavaScorerOp.h'
             ])
    ),
    deps=([
        ':version_h', ':ha3_isearch_h', ':ha3_proto_headers',
        ':ha3_fbs_headers', '//navi:navi',
        '//aios/ha3/ha3/common:ha3_common_headers',
        '//aios/ha3/ha3/queryparser:ha3_queryparser_headers',
        '//aios/ha3/ha3/queryparser/clause:ha3_queryparser_clause_headers',
        '//aios/ha3/ha3/search/query_executor:ha3_query_executor_headers',
        '//aios/ha3/ha3/search/auxiliary_chain:ha3_auxiliary_chain_headers',
        '//aios/ha3/ha3/search/single_layer_searcher:ha3_single_layer_searcher_headers',
        '//aios/ha3/ha3/search/filter:ha3_filter_headers',
        '//aios/ha3/ha3/rank/comparator:ha3_comparator_headers',
        '//aios/ha3/ha3/turing/common/metadata:modelconfig',
        '//aios/ha3/ha3/turing/common/metadata:ha3_meta_data',
        '//aios/ha3/ha3/monitor:ha3_monitor_headers',
        '//aios/ha3/ha3/util/memcache:ha3_memcache_headers',
        '//aios/ha3/ha3/sql/config:sql_config',
        '//aios/ha3/ha3/sql/ops/agg:sql_ops_agg_base',
        '//aios/ha3/ha3/sql/ops/tvf:sql_ops_tvf_base',
        '//aios/resource_reader:resource_reader',
        '@suez_turing.turbojet.turing_ops_util//:suez_turing',
        '//aios/autil:timeout_terminator',
        '//aios/worker_framework:worker_framework',
        '//aios/sql/iquan/cpp/common/catalog:iquan_catalog_client',
        '//aios/network/gig:multi_call', '//aios/table:table',
        '//aios/autil:object_tracer'
    ] + []),
    visibility=['//visibility:public'],
    include_prefix='ha3',
    strip_include_prefix='ha3'
)
cc_library(
    name='ha3_sql_lib',
    deps=[
        ':ha3_sql_lib_base',
        '//aios/ha3/ha3/sql/ops/tvf/builtin/summary_tvf:sql_ops_tvf_summary',
        '//aios/ha3/ha3/sql/ops/tvf/builtin/score_model_tvf:sql_ops_tvf_score_model',
        '//aios/ha3/ha3/sql/ops/scan/builtin/aitheta_udf_to_query:sql_ops_aitheta_udf_to_query',
        '//aios/ha3/ha3/sql/ops/planOptimize/model/kernel:sql_ops_plan_optimize',
        '//aios/ha3/ha3/sql/framework:sql_framework'
    ],
    visibility=['//visibility:public']
)
cc_library(
    name='ha3_sql_lib_base',
    deps=([
        '//aios/ha3/ha3/sql/common:sql_common',
        '//aios/ha3/ha3/sql/proto:sql_proto',
        '//aios/ha3/ha3/sql/resource:sql_resource',
        '//aios/ha3/ha3/sql/data:sql_data',
        '//aios/ha3/ha3/sql/config:sql_config',
        '//aios/ha3/ha3/sql/ops/util:sql_ops_util',
        '//aios/ha3/ha3/sql/ops/condition:sql_ops_condition',
        '//aios/ha3/ha3/sql/ops/scan/kernel:sql_ops_scan',
        '//aios/ha3/ha3/sql/ops/calc/kernel:sql_ops_calc',
        '//aios/ha3/ha3/sql/ops/turbojetCalc/kernel:sql_ops_turbojet_calc',
        '//aios/ha3/ha3/sql/ops/correlate/kernel:sql_ops_correlate',
        '//aios/ha3/ha3/sql/ops/values/kernel:sql_ops_values',
        '//aios/ha3/ha3/sql/ops/agg/kernel:sql_ops_agg',
        '//aios/ha3/ha3/sql/ops/tvf/kernel:sql_ops_tvf',
        '//aios/ha3/ha3/sql/ops/sort/kernel:sql_ops_sort',
        '//aios/ha3/ha3/sql/ops/limit/kernel:sql_ops_limit',
        '//aios/ha3/ha3/sql/ops/union/kernel:sql_ops_union',
        '//aios/ha3/ha3/sql/ops/sink/kernel:sql_ops_sink',
        '//aios/ha3/ha3/sql/ops/join/kernel:sql_ops_join',
        '//aios/ha3/ha3/sql/ops/identity/kernel:sql_ops_identity',
        '//aios/ha3/ha3/sql/ops/tableModify/kernel:sql_ops_table_modify',
        '//aios/ha3/ha3/sql/ops/agg/builtin:sql_ops_agg_builtin',
        '//aios/ha3/ha3/sql/ops/orcScan:sql_ops_orc_scan_kernel',
        '//aios/ha3/ha3/sql/ops/parser/kernel:sql_ops_parser',
        '//aios/ha3/ha3/sql/ops/planTransform/kernel:sql_ops_plan_transform',
        '//aios/ha3/ha3/sql/ops/runSqlGraph/kernel:sql_ops_run_sql_graph_kernel',
        '//aios/ha3/ha3/sql/ops/tableMerge/kernel:sql_ops_table_merge',
        '//aios/ha3/ha3/sql/ops/tableSplit/kernel:sql_ops_table_split',
        '//aios/ha3/ha3/sql/ops/delayDp/kernel:sql_ops_delay_dp'
    ] + []),
    visibility=['//visibility:public']
)
cc_library(
    name='ha3_model_handler',
    hdrs=['ha3/service/QrsModelHandler.h'],
    srcs=glob(['ha3/service/QrsModelHandler.cpp']),
    deps=[
        ':ha3_isearch_h', '//aios/ha3/ha3/turing/common/metadata:modelconfig',
        '//aios/network/gig:multi_call',
        '@org_tensorflow//tensorflow/core:direct_session',
        '//aios/ha3/ha3/common:ha3_tracer', '//aios/ha3/ha3/common:common_def',
        '@suez_turing.turbojet.turing_ops_util//:suez_turing',
        '@suez_turing.turbojet.turing_ops_util//:turing_ops_util'
    ],
    copts=['-Werror'],
    visibility=['//visibility:public'],
    include_prefix='ha3',
    strip_include_prefix='ha3'
)
cc_library(
    name='ha3_framework',
    srcs=(
        glob([
            'ha3/schema/*.cpp', 'ha3/config/*.cpp', 'ha3/index/*.cpp',
            'ha3/search/*.cpp', 'ha3/service/*.cpp', 'ha3/worker/*.cpp',
            'ha3/proxy/*.cpp', 'ha3/func_expression/*.cpp', 'ha3/sorter/*.cpp',
            'ha3/worker/*.cpp', 'ha3/turing/variant/*.cpp',
            'ha3/turing/common/*.cpp', 'ha3/turing/qrs/*.cpp',
            'ha3/turing/searcher/*.cpp', 'ha3/turing/suez_navi/*.cpp',
            'ha3/cava/*.cpp', 'ha3/cava/scorer/*.cpp'
        ],
             exclude=[
                 'ha3/service/QrsModelHandler.cpp',
                 'ha3/service/SqlModelGraphOptimizer.cpp'
             ]) +
        glob(['ha3/qrs/*.cpp', 'ha3/summary/*.cpp', 'ha3/rank/*.cpp'],
             exclude=[
                 'ha3/qrs/AdapterQrsProcessor.cpp',
                 'ha3/summary/AdapterSummary.cpp', 'ha3/rank/AdapterScorer.cpp'
             ])
    ),
    deps=([
        ':ha3_framework_headers', ':ha3_proto', ':ha3_sql_lib', ':ha3_util',
        '//aios/ha3/ha3/common/query:ha3_query',
        '//aios/ha3/ha3/turing/common/metadata:ha3_meta_data',
        '//aios/ha3/ha3/queryparser:ha3_queryparser',
        '//aios/ha3/ha3/queryparser/clause:ha3_queryparser_clause',
        '//aios/ha3/ha3/search/query_executor:ha3_query_executor',
        '//aios/ha3/ha3/search/auxiliary_chain:ha3_auxiliary_chain',
        '//aios/ha3/ha3/search/single_layer_searcher:ha3_single_layer_searcher',
        '//aios/ha3/ha3/search/filter:ha3_filter',
        '//aios/ha3/ha3/rank/comparator:ha3_comparator',
        '@suez_turing.turbojet.turing_ops_util//:suez_turing',
        '//aios/suez:suez_search_sdk', '//aios/sql/iquan/cpp/jni:iquan_jni',
        '@suez_turing.turbojet.turing_ops_util//:turing_ops_util',
        '//aios/basic_ops:qinfo_variant', '//navi:navi',
        '//third_party/mxml:mxml', '//aios/autil:range_util',
        '//aios/storage/indexlib:tablet'
    ] + []),
    implementation_deps=[
        '//aios/ha3/ha3/common:ha3_common',
        '//aios/ha3/ha3/monitor:ha3_monitor',
        '//aios/ha3/ha3/util/memcache:ha3_memcache'
    ],
    copts=['-Werror'],
    visibility=['//visibility:public'],
    include_prefix='ha3',
    strip_include_prefix='ha3',
    alwayslink=1
)
config_setting(name='compile_with_asan', values={'define': 'compileMode=asan'})
cc_binary(
    name='havenask',
    srcs=glob([
        'ha3/turing/ServiceCreator.cpp', 'ha3/application/Server.h',
        'ha3/application/Server.cpp', 'ha3/application/main.cpp'
    ]),
    deps=[
        ':ha3_turing_ops', ':ha3_framework',
        '@org_tensorflow//tensorflow/core:all_kernels',
        '//aios/suez:suez_worker', '//suez_navi:suez_navi'
    ],
    linkopts=select({
        ':compile_with_asan': ['-pie', '-mcmodel=large'],
        '//conditions:default': []
    }),
    visibility=['//visibility:public'],
    tags=['manual', 'no-remote-exec']
)
bundle_files(
    name='ha3_etc_package',
    prefix='usr/local/etc/ha3/',
    srcs=(
        glob(['misc/*', 'misc/agg/*'],
             exclude=[
                 'misc/searcher_default.def', 'misc/searcher_default.pbtxt',
                 'misc/havenask_searcher_default.def',
                 'misc/havenask_searcher_default.pbtxt'
             ]) +
        [':havenask_searcher_default.pbtxt', ':havenask_searcher_default.def']
    ),
    strip_prefix='misc',
    visibility=['//visibility:public'],
    tags=['manual']
)
copy_target_to(
    name='havenask_searcher_default.def',
    to_copy='//aios/ha3:misc/havenask_searcher_default.def',
    copy_name='searcher_default.def',
    dests=['misc/']
)
copy_target_to(
    name='havenask_searcher_default.pbtxt',
    to_copy='//aios/ha3:misc/havenask_searcher_default.pbtxt',
    copy_name='searcher_default.pbtxt',
    dests=['misc/']
)
gen_conf_lib(
    name='version_h',
    srcs=['ha3/version.h.in'],
    outs=['ha3/version.h'],
    args={
        '@HA_BUILD_SIGNATURE@': 'ha3_3_8_0',
        '@HA_PROTOCOL_VERSION@': 'ha3-protocol-3.0'
    },
    strip_include_prefix='ha3',
    include_prefix='ha3'
)
cc_proto(
    name='ha3_proto_basic_def',
    srcs=['ha3/proto/BasicDefs.proto'],
    deps=[],
    import_prefix='ha3/proto/',
    strip_import_prefix='ha3/proto/'
)
cc_proto(
    name='ha3_proto_rpc',
    srcs=['ha3/proto/QrsService.proto'],
    deps=[':ha3_proto_basic_def', '//aios/network/arpc:proto'],
    import_prefix='ha3/proto/',
    strip_import_prefix='ha3/proto/',
    visibility=['//visibility:public']
)
flatbuffer_library_public(
    name='ha3_fbs',
    srcs=[
        'ha3/proto/TsdbColumn.fbs', 'ha3/proto/TwoDimTable.fbs',
        'ha3/proto/SqlResult.fbs', 'ha3/proto/SummaryResult.fbs'
    ],
    outs=[
        'ha3/proto/TsdbColumn_generated.h', 'ha3/proto/TwoDimTable_generated.h',
        'ha3/proto/SqlResult_generated.h', 'ha3/proto/SummaryResult_generated.h'
    ],
    out_prefix='ha3/proto/',
    language_flag='-c'
)
cc_library(
    name='ha3_fbs_headers',
    hdrs=[':ha3_fbs'],
    deps=['@flatbuffers//:flatbuffers'],
    include_prefix='ha3',
    strip_include_prefix='ha3',
    visibility=['//visibility:public']
)
cc_library(
    name='ha3_isearch_h',
    hdrs=['ha3/isearch.h'],
    srcs=['ha3/isearch.cpp'],
    deps=[
        '//aios/autil:long_hash_value', '//aios/matchdoc:matchdoc',
        '//aios/storage/indexlib/indexlib:indexlib_h'
    ],
    include_prefix='ha3',
    strip_include_prefix='ha3'
)
cc_library(
    name='ha3_proto_headers',
    hdrs=glob(['ha3/proto/*.h']),
    deps=[
        ':ha3_isearch_h', ':ha3_proto_basic_def_cc_proto_headers',
        ':ha3_proto_rpc_cc_proto_headers'
    ],
    include_prefix='ha3',
    strip_include_prefix='ha3'
)
cc_library(
    name='ha3_proto',
    srcs=glob(['ha3/proto/*.cpp']),
    deps=[
        ':ha3_proto_headers', ':ha3_proto_basic_def_cc_proto',
        ':ha3_proto_rpc_cc_proto', '//aios/autil:compression'
    ],
    visibility=['//visibility:public']
)
cc_library(
    name='ha3_turing_ops',
    srcs=glob(['ha3/turing/ops/*.cpp', 'ha3/turing/ops/agg/*.cpp'],
              exclude=['ha3/turing/ops/Ha3CavaScorerOp.cpp']),
    implementation_deps=(
        ['@suez_turing.turbojet.turing_ops_util//:turing_ops_util'] + []
    ),
    deps=[
        ':ha3_framework_headers',
        '@suez_turing.turbojet.turing_ops_util//:suez_turing',
        '@org_tensorflow//tensorflow/core:direct_session'
    ],
    copts=['-Werror'],
    include_prefix='ha3',
    strip_include_prefix='ha3',
    alwayslink=True
)
filegroup(
    name='ha_tools_files',
    srcs=glob(['tools/ha_tools/*', 'tools/ha_tools/agg/*']),
    visibility=['//visibility:public']
)
bundle_files(
    name='ha_tools_package',
    prefix='usr/local/lib/python/site-packages/ha_tools',
    srcs=[':ha_tools_files'],
    deps=[],
    strip_prefix='/aios/ha3/tools/ha_tools',
    visibility=['//visibility:public']
)
flatbuffer_library_public(
    name='ha3_fbs_py',
    srcs=[
        'ha3/proto/TsdbColumn.fbs', 'ha3/proto/TwoDimTable.fbs',
        'ha3/proto/SqlResult.fbs', 'ha3/proto/SummaryResult.fbs'
    ],
    outs=[
        'ha3/proto/isearch/fbs/Column.py',
        'ha3/proto/isearch/fbs/ColumnType.py',
        'ha3/proto/isearch/fbs/DoubleColumn.py',
        'ha3/proto/isearch/fbs/FBErrorResult.py',
        'ha3/proto/isearch/fbs/FloatColumn.py',
        'ha3/proto/isearch/fbs/Int16Column.py',
        'ha3/proto/isearch/fbs/Int32Column.py',
        'ha3/proto/isearch/fbs/Int64Column.py',
        'ha3/proto/isearch/fbs/Int8Column.py',
        'ha3/proto/isearch/fbs/MultiDouble.py',
        'ha3/proto/isearch/fbs/MultiDoubleColumn.py',
        'ha3/proto/isearch/fbs/MultiFloat.py',
        'ha3/proto/isearch/fbs/MultiFloatColumn.py',
        'ha3/proto/isearch/fbs/MultiInt16.py',
        'ha3/proto/isearch/fbs/MultiInt16Column.py',
        'ha3/proto/isearch/fbs/MultiInt32.py',
        'ha3/proto/isearch/fbs/MultiInt32Column.py',
        'ha3/proto/isearch/fbs/MultiInt64.py',
        'ha3/proto/isearch/fbs/MultiInt64Column.py',
        'ha3/proto/isearch/fbs/MultiInt8.py',
        'ha3/proto/isearch/fbs/MultiInt8Column.py',
        'ha3/proto/isearch/fbs/MultiString.py',
        'ha3/proto/isearch/fbs/MultiStringColumn.py',
        'ha3/proto/isearch/fbs/MultiUInt16.py',
        'ha3/proto/isearch/fbs/MultiUInt16Column.py',
        'ha3/proto/isearch/fbs/MultiUInt32.py',
        'ha3/proto/isearch/fbs/MultiUInt32Column.py',
        'ha3/proto/isearch/fbs/MultiUInt64.py',
        'ha3/proto/isearch/fbs/MultiUInt64Column.py',
        'ha3/proto/isearch/fbs/MultiUInt8.py',
        'ha3/proto/isearch/fbs/MultiUInt8Column.py',
        'ha3/proto/isearch/fbs/SqlErrorResult.py',
        'ha3/proto/isearch/fbs/SqlResult.py',
        'ha3/proto/isearch/fbs/StringColumn.py',
        'ha3/proto/isearch/fbs/SummaryResult.py',
        'ha3/proto/isearch/fbs/TwoDimTable.py',
        'ha3/proto/isearch/fbs/UInt16Column.py',
        'ha3/proto/isearch/fbs/UInt32Column.py',
        'ha3/proto/isearch/fbs/UInt64Column.py',
        'ha3/proto/isearch/fbs/UInt8Column.py',
        'ha3/proto/isearch/fbs/ByteString.py',
        'ha3/proto/isearch/fbs/ByteStringColumn.py',
        'ha3/proto/isearch/fbs/MultiByteString.py',
        'ha3/proto/isearch/fbs/MultiByteStringColumn.py',
        'ha3/proto/isearch/fbs/TsdbDataPoint.py',
        'ha3/proto/isearch/fbs/TsdbDataPointSeries.py',
        'ha3/proto/isearch/fbs/TsdbStringPointSeries.py',
        'ha3/proto/isearch/fbs/TsdbDpsColumn.py',
        'ha3/proto/isearch/fbs/TsdbStringDpsColumn.py',
        'ha3/proto/isearch/fbs/__init__.py'
    ],
    out_prefix='ha3/proto/',
    language_flag='--python'
)
gen_conf_lib(
    name='ha3_test_h',
    srcs=['ha3/test/test.h.in'],
    outs=['ha3/test/test.h'],
    args={
        '@TOP_SRCDIR@': 'aios/ha3',
        '@TOP_BUILDDIR@': '.',
        '@BUILDDIR@': '.',
        '@INSTALLROOT@': '.'
    },
    strip_include_prefix='ha3',
    include_prefix='ha3'
)
filegroup(
    name='testdata',
    srcs=glob(['testdata/**', 'misc/leak_suppression']),
    data=['leak_suppression']
)
filegroup(name='leak_suppression', srcs=glob(['misc/leak_suppression']))
