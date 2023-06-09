package(default_visibility=['//aios/ha3:__subpackages__'])
load('//bazel:defs.bzl', 'cc_proto')
cc_proto(
    name='sql_search_info_proto',
    srcs=['SqlSearchInfo.proto', 'SqlSearch.proto'],
    deps=['//aios/network/arpc:proto'],
    import_prefix='ha3/sql/proto',
    visibility=['//visibility:public']
)
cc_library(
    name='sql_proto',
    srcs=glob(['SqlSearchInfoCollector.cpp']),
    hdrs=glob(['SqlSearchInfoCollector.h']),
    include_prefix='ha3/sql/proto',
    deps=[
        ':sql_search_info_proto_cc_proto_headers', ':sql_search_info_proto_cc',
        '//aios/ha3/ha3/sql/common:sql_log'
    ],
    alwayslink=True
)
