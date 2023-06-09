package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_scan_base',
    srcs=glob(['*.cpp'],
              exclude=[
                  'UdfToQueryManager.cpp', 'Ha3ScanConditionVisitor.cpp',
                  'ScanIteratorCreator.cpp', 'NormalScan.cpp'
              ]),
    hdrs=glob(['*.h', '*.hpp'],
              exclude=[
                  'UdfToQueryManager.h', 'Ha3ScanConditionVisitor.h',
                  'ScanIteratorCreator.h', 'NormalScan.h'
              ]),
    include_prefix='ha3/sql/ops/scan',
    deps=[
        '//aios/ha3/ha3/sql/resource:sql_resource',
        '//aios/ha3/ha3/sql/common:sql_common',
        '//aios/ha3/ha3/search/single_layer_searcher:ha3_single_layer_searcher',
        '//aios/ha3/ha3/rank/comparator:ha3_comparator',
        '//aios/ha3/ha3/turing/common/metadata:modelconfig',
        '//aios/ha3/ha3/turing/common/metadata:sort_descriptions',
        '//aios/ha3/ha3/queryparser:ha3_queryparser',
        '//aios/ha3/ha3/search/auxiliary_chain:ha3_auxiliary_chain',
        '//aios/ha3/ha3/sql/ops/calc:sql_ops_calc_table',
        '//aios/ha3/ha3/sql/ops/sort:sql_ops_sort_init_param',
        '//aios/ha3/ha3/sql/ops/util:sql_ops_util',
        '//aios/ha3:ha3_proto_basic_def_cc_proto_headers',
        '//aios/storage/indexlib/indexlib/index:kkv',
        '//aios/storage/indexlib/table/kv_table:table',
        '//aios/suez:suez_search_sdk', '//aios/suez:table_service_cc_proto',
        '//aios/autil:plugin_base', '//aios/autil:range_util',
        '//aios/autil:object_tracer', '//navi_ops:navi_ops'
    ],
    alwayslink=True
)
cc_library(
    name='sql_ops_scan_udftoquery_manager',
    srcs=glob(['UdfToQueryManager.cpp']),
    hdrs=glob(['UdfToQueryManager.h']),
    include_prefix='ha3/sql/ops/scan',
    deps=[
        '//aios/ha3/ha3/sql/ops/scan/builtin:sql_ops_udf_to_query_builtin',
        '//aios/autil:plugin_base', '//aios/autil:log'
    ],
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='sql_ops_scan_visitor',
    srcs=glob(['Ha3ScanConditionVisitor.cpp', 'ScanIteratorCreator.cpp']),
    hdrs=glob(['Ha3ScanConditionVisitor.h', 'ScanIteratorCreator.h']),
    include_prefix='ha3/sql/ops/scan',
    deps=[
        '//aios/ha3/ha3/sql/ops/scan:sql_ops_scan_udftoquery_manager',
        '//aios/ha3/ha3/sql/resource:sql_resource',
        '//aios/ha3/ha3/queryparser:ha3_queryparser',
        '//aios/ha3/ha3/sql/common:sql_common'
    ],
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='sql_ops_normal_scan',
    srcs=glob(['NormalScan.cpp']),
    hdrs=glob(['NormalScan.h']),
    include_prefix='ha3/sql/ops/scan',
    deps=[
        '//aios/ha3/ha3/sql/ops/scan:sql_ops_scan_visitor',
        '//aios/ha3/ha3/sql/resource:sql_resource',
        '//aios/ha3/ha3/queryparser:ha3_queryparser',
        '//aios/ha3/ha3/sql/common:sql_common'
    ],
    visibility=['//visibility:public'],
    alwayslink=True
)
