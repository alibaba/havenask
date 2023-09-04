package(default_visibility=['//aios/sql:__subpackages__'])
load('//bazel:defs.bzl', 'cc_proto')
cc_proto(
    name='sql_search_info_proto',
    srcs=['SqlSearch.proto', 'SqlSearchInfo.proto'],
    import_prefix='sql/proto',
    visibility=['//visibility:public'],
    deps=['//aios/network/arpc:proto']
)
cc_library(
    name='sql_proto',
    srcs=glob(['SqlSearchInfoCollector.cpp', 'SqlSearchInfoCollectorR.cpp']),
    hdrs=glob(['SqlSearchInfoCollector.h', 'SqlSearchInfoCollectorR.h']),
    include_prefix='sql/proto',
    deps=[
        ':sql_search_info_proto_cc', ':sql_search_info_proto_cc_proto_headers',
        '//aios/sql/common:sql_log'
    ],
    alwayslink=True
)
