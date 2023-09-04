package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_agg_builtin',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/ops/agg/builtin',
    deps=[
        '//aios/apps/facility/build_service:bs_plugin',
        '//aios/autil:plugin_base', '//aios/sql/common:sql_log',
        '//aios/sql/iquan/cpp/common:iquan_common',
        '//aios/sql/ops/agg:sql_ops_agg_base', '//aios/table'
    ],
    alwayslink=True
)
cc_test(
    name='ha3_agg_builtin_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[],
    deps=[
        ':sql_ops_agg_builtin', '//aios/ha3:ha3_test_h',
        '//aios/sql/ops/agg:sql_ops_agg_func_factory',
        '//aios/sql/ops/test:ops_testlib', '//aios/unittest_framework'
    ]
)
