package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_limit',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/ops/limit',
    deps=[
        '//aios/navi', '//aios/sql/ops/util:sql_ops_util',
        '//aios/sql/resource:sql_resource', '//aios/table'
    ],
    alwayslink=True
)
cc_test(
    name='ha3_sql_limit_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[],
    deps=[
        ':sql_ops_limit', '//aios/ha3:ha3_test_h',
        '//aios/sql/ops/test:ops_testlib', '//aios/unittest_framework'
    ]
)
