package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_calc_table',
    srcs=glob(['*.cpp'], exclude=['CalcR.cpp']),
    hdrs=glob(['*.h'], exclude=['CalcR.h']),
    include_prefix='sql/ops/calc',
    deps=[
        '//aios/autil:plugin_base', '//aios/navi',
        '//aios/sql/framework:sql_push_down_op',
        '//aios/sql/ops/condition:sql_ops_condition',
        '//aios/sql/ops/util:sql_ops_util', '//aios/sql/proto:sql_proto',
        '//aios/sql/resource:sql_resource', '//aios/table'
    ]
)
cc_test(
    name='test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    deps=[
        ':sql_ops_calc_table', '//aios/navi:navi_log_testlib',
        '//aios/navi:navi_ops_testlib', '//aios/table/test:table_testlib',
        '//aios/unittest_framework'
    ]
)
