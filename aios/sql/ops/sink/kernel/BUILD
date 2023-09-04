package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_sink',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/ops/sink',
    deps=[
        '//aios/navi', '//aios/sql/ops/util:sql_ops_util',
        '//aios/sql/resource:sql_resource', '//aios/table'
    ],
    alwayslink=True
)
cc_test(
    name='ha3_sql_sink_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[],
    deps=[
        ':sql_ops_sink', '//aios/ha3:ha3_test_h',
        '//aios/sql/ops/test:ops_testlib', '//aios/unittest_framework'
    ]
)
