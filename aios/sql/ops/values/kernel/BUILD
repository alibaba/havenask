package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_values',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/ops/values',
    deps=['//aios/navi', '//aios/sql/ops/util:sql_ops_util'],
    alwayslink=True
)
cc_test(
    name='ha3_sql_values_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[],
    deps=[
        ':sql_ops_values', '//aios/ha3:ha3_test_h',
        '//aios/sql/ops/test:ops_testlib', '//aios/unittest_framework'
    ]
)
