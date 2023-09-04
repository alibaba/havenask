package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_table_modify_base',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/ops/tableModify',
    deps=[
        '//aios/autil:json_helper', '//aios/navi',
        '//aios/sql/data:sql_table_data',
        '//aios/sql/ops/condition:sql_ops_condition',
        '//aios/sql/ops/util:sql_ops_util', '//aios/sql/resource/message_writer'
    ],
    alwayslink=True
)
cc_test(
    name='ha3_sql_table_modify_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    deps=[
        ':sql_ops_table_modify_base', '//aios/ha3:ha3_test_h',
        '//aios/navi:navi_graph_runner_testlib', '//aios/navi:navi_log_testlib',
        '//aios/sql/ops/test:ops_testlib', '//aios/unittest_framework'
    ]
)
