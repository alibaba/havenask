package(default_visibility=['//aios/ha3:__subpackages__'])
load('//bazel:defs.bzl', 'genyacc', 'genlex')
genlex(name='sp_query_scanner', src='SpQueryScanner.ll')
genyacc(
    name='sp_query_parser',
    src='SpQueryParser.yy',
    path='ha3/sql/ops/scan/builtin/'
)
cc_library(
    name='sql_ops_udf_to_query_builtin',
    srcs=(glob(['*.cpp']) + [':sp_query_scanner', ':sp_query_parser_srcs']),
    hdrs=glob(['*.h']),
    deps=[
        ':sp_query_parser_hdrs',
        '//aios/ha3/ha3/sql/ops/scan:sql_ops_scan_base',
        '//aios/ha3/ha3/search/single_layer_searcher:ha3_single_layer_searcher',
        '//aios/ha3/ha3/queryparser:ha3_queryparser',
        '//aios/ha3/ha3/sql/ops/calc:sql_ops_calc_table',
        '//aios/ha3/ha3/sql/ops/util:sql_ops_util'
    ],
    include_prefix='ha3/sql/ops/scan/builtin'
)
