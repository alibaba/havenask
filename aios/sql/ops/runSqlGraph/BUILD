package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_run_sql_graph',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/ops/runSqlGraph',
    deps=[
        '//aios/autil:range_util', '//aios/navi',
        '//aios/sql/resource:sql_resource'
    ],
    alwayslink=True
)
cc_test(
    name='sql_ops_run_sql_graph_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[],
    deps=[
        ':sql_ops_run_sql_graph', '//aios/ha3:ha3_test_h',
        '//aios/navi:navi_log_testlib', '//aios/unittest_framework'
    ]
)
