package(default_visibility=['//aios/sql:__subpackages__'])
load('//bazel:defs.bzl', 'genlex', 'genyacc')
genlex(name='sp_query_scanner', src='SpQueryScanner.ll')
genyacc(
    name='sp_query_parser',
    src='SpQueryParser.yy',
    path='sql/ops/scan/udf_to_query/'
)
cc_library(
    name='sql_ops_udf_to_query',
    srcs=(
        glob(['*.cpp'], exclude=['*Aitheta*.cpp']) +
        [':sp_query_scanner', ':sp_query_parser_srcs']
    ),
    hdrs=glob(['*.h'], exclude=['*Aitheta*.h']),
    include_prefix='sql/ops/scan/udf_to_query',
    deps=[
        ':sp_query_parser_hdrs', '//aios/autil:json_helper',
        '//aios/ha3/ha3/queryparser:ha3_queryparser',
        '//aios/ha3/ha3/search/single_layer_searcher:ha3_single_layer_searcher',
        '//aios/ha3/ha3/turing/common/metadata:modelconfig',
        '//aios/sql/ops/condition:sql_ops_condition',
        '//aios/sql/ops/util:sql_ops_util'
    ]
)
cc_test(
    name='udf_to_query_test',
    srcs=glob(['test/*Test.cpp'], exclude=['test/*Aitheta*.cpp']),
    copts=['-fno-access-control'],
    data=[],
    deps=[':sql_ops_udf_to_query', '//aios/sql/ops/scan:condition_test_util']
)
