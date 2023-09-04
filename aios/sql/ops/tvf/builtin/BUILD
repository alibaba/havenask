package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_tvf_builtin',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/ops/tvf/builtin',
    deps=[
        '//aios/apps/facility/build_service:bs_plugin',
        '//aios/sql/iquan/cpp/common:iquan_common',
        '//aios/sql/ops/tvf:sql_ops_tvf_base', '//aios/table'
    ],
    alwayslink=True
)
cc_test(
    name='ha3_sql_tvf_builtin_test',
    srcs=glob(['test/*Test.cpp'],
              exclude=['test/RerankByQuotaTvfFuncTest.cpp']),
    copts=['-fno-access-control'],
    data=[],
    deps=[
        ':sql_ops_tvf_builtin', '//aios/ha3:ha3_test_h',
        '//aios/sql/ops/test:ops_testlib', '//aios/unittest_framework'
    ]
)
