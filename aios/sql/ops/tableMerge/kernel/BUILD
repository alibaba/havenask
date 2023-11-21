package(
    default_visibility=[
        '//aios/sql:__subpackages__',
        '//aios/apps/engine/vector_service:__subpackages__'
    ]
)
cc_library(
    name='sql_ops_table_merge',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/ops/tableMerge/kernel',
    deps=[
        '//aios/navi', '//aios/sql/data:sql_table_data',
        '//aios/sql/ops/util:sql_ops_util'
    ],
    alwayslink=True
)
filegroup(name='testdata', testonly=True, srcs=glob(['testdata/**']))
cc_test(
    name='ha3_sql_table_merge_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[':testdata'],
    deps=[
        ':sql_ops_table_merge', '//aios/ha3:ha3_test_h',
        '//aios/navi:navi_graph_runner_testlib', '//aios/navi:navi_log_testlib',
        '//aios/sql/ops/test:ops_testlib', '//aios/unittest_framework'
    ]
)
