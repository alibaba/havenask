package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_correlate',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/ops/correlate',
    deps=[
        '//aios/navi', '//aios/sql/ops/util:sql_ops_util',
        '//aios/sql/proto:sql_proto', '//aios/sql/resource:sql_resource',
        '//aios/table'
    ],
    alwayslink=True
)
cc_test(
    name='ha3_sql_correlate_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[],
    deps=[
        ':sql_ops_correlate', '//aios/ha3:ha3_test_h',
        '//aios/sql/ops/test:ops_testlib', '//aios/unittest_framework'
    ]
)
