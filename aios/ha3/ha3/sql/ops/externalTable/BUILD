package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_external_gig',
    srcs=['GigQuerySessionClosure.cpp', 'GigQuerySessionCallbackCtx.cpp'],
    hdrs=['GigQuerySessionClosure.h', 'GigQuerySessionCallbackCtx.h'],
    include_prefix='ha3/sql/ops/externalTable',
    deps=[
        '//aios/ha3/ha3/sql/common:sql_common',
        '//aios/ha3/ha3/sql/ops/util:sql_ops_util',
        '//aios/network/gig:multi_call'
    ],
    alwayslink=True
)
