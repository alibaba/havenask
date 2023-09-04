package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_calc',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/ops/calc',
    deps=[
        '//aios/navi', '//aios/sql/ops/calc:sql_ops_calc_table',
        '//aios/sql/ops/util:sql_ops_util', '//aios/sql/proto:sql_proto',
        '//aios/sql/resource:sql_resource'
    ],
    alwayslink=True
)
cc_test(
    name='test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[],
    deps=[
        ':sql_ops_calc', '//aios/sql/ops/test:ops_testlib',
        '//aios/unittest_framework'
    ]
)
