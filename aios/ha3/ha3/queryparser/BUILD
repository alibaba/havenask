package(default_visibility=['//aios/ha3:__subpackages__'])
load('//bazel:defs.bzl', 'genlex', 'genyacc')
genlex(name='ha3_scanner', src='Scanner.ll')
genyacc(name='ha3_parser', src='BisonParser.yy', path='ha3/queryparser/')
cc_library(
    name='ha3_queryparser_headers',
    hdrs=(glob(['*.h']) + []),
    include_prefix='ha3/queryparser',
    deps=[':ha3_parser_hdrs', '//aios/ha3/ha3/common/query:ha3_query_headers']
)
cc_library(
    name='ha3_queryparser',
    srcs=(glob(['*.cpp']) + [':ha3_scanner', ':ha3_parser_srcs']),
    visibility=['//visibility:public'],
    deps=[':ha3_queryparser_headers', '//aios/ha3/ha3/common/query:ha3_query'],
    alwayslink=1
)
