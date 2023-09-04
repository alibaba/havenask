package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_external_gig',
    srcs=['GigQuerySessionCallbackCtxR.cpp', 'GigQuerySessionClosure.cpp'],
    hdrs=['GigQuerySessionCallbackCtxR.h', 'GigQuerySessionClosure.h'],
    include_prefix='sql/ops/externalTable',
    deps=[
        '//aios/network/gig:multi_call', '//aios/sql/common:sql_common',
        '//aios/sql/ops/util:sql_ops_util'
    ],
    alwayslink=True
)
cc_test(
    name='test',
    srcs=glob(['test/*.cpp']),
    copts=['-fno-access-control'],
    linkopts=['-Wl,--as-needed'],
    deps=[
        ':sql_ops_external_gig', '//aios/navi:navi_engine_testlib',
        '//aios/unittest_framework'
    ]
)
