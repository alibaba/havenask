package(default_visibility=['//aios/ha3:__subpackages__'])
load('//bazel:defs.bzl', 'genyacc', 'genlex')
genlex(name='ha3_clause_scanner', src='ClauseScanner.ll')
genyacc(
    name='ha3_clause_parser',
    src='ClauseBisonParser.yy',
    path='ha3/queryparser/ClauseBisonParser/'
)
cc_library(
    name='ha3_queryparser_clause_headers',
    hdrs=(glob(['*.h']) + []),
    deps=[
        ':ha3_clause_parser_hdrs', '//aios/autil:log',
        '@suez_turing.turbojet.turing_ops_util//:suez_turing',
        '//aios/ha3/ha3/common:ha3_error_result_headers',
        '//aios/ha3/ha3/common:ha3_common_headers',
        '//aios/ha3/ha3/queryparser:ha3_queryparser_headers'
    ],
    include_prefix='ha3/queryparser'
)
cc_library(
    name='ha3_queryparser_clause',
    srcs=(glob(['*.cpp']) + [':ha3_clause_scanner', ':ha3_clause_parser_srcs']),
    deps=[
        ':ha3_queryparser_clause_headers',
        '//aios/ha3/ha3/common:ha3_error_result',
        '//aios/ha3/ha3/common:ha3_common',
        '@suez_turing.turbojet.turing_ops_util//:suez_turing',
        '//aios/ha3/ha3/queryparser:ha3_queryparser'
    ]
)
