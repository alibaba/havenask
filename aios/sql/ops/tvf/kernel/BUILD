package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_tvf',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/ops/tvf',
    deps=[
        '//aios/navi', '//aios/sql/ops/tvf:sql_ops_tvf_wrapper',
        '//aios/sql/proto:sql_proto', '//aios/sql/resource:sql_resource'
    ],
    alwayslink=True
)
cc_test(
    name='ha3_sql_tvf_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[],
    deps=[
        ':sql_ops_tvf', '//aios/ha3:ha3_test_h',
        '//aios/sql/ops/test:ops_testlib', '//aios/unittest_framework'
    ]
)
