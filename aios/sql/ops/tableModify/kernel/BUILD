package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_table_modify',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/ops/tableModify/kernel',
    deps=[
        '//aios/autil:json_helper', '//aios/navi',
        '//aios/sql/ops/calc:sql_ops_calc_table',
        '//aios/sql/ops/tableModify:sql_ops_table_modify_base',
        '//aios/sql/ops/util:sql_ops_util', '//aios/sql/resource:sql_resource'
    ],
    alwayslink=True
)
cc_test(
    name='ha3_sql_table_modify_kernel_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    deps=[
        ':sql_ops_table_modify',
        '//aios/apps/facility/swift/testlib:mock_swift_client',
        '//aios/ha3:ha3_test_h', '//aios/navi:navi_graph_runner_testlib',
        '//aios/navi:navi_log_testlib', '//aios/sql/ops/test:ops_testlib',
        '//aios/unittest_framework'
    ]
)
