package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_agg',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/ops/agg',
    deps=[
        '//aios/navi', '//aios/sql/ops/agg:sql_ops_agg_base',
        '//aios/sql/ops/agg:sql_ops_agg_func_factory',
        '//aios/sql/ops/agg:sql_ops_agg_op',
        '//aios/sql/ops/calc:sql_ops_calc_table'
    ],
    alwayslink=True
)
cc_test(
    name='ha3_sql_agg_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[],
    deps=[
        ':sql_ops_agg', '//aios/sql/ops/test:ops_testlib',
        '//aios/unittest_framework'
    ]
)
