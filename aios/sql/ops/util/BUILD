package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_util',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/ops/util',
    deps=[
        '//aios/autil:object_tracer', '//aios/matchdoc',
        '//aios/sql/common:sql_common', '//aios/sql/data:sql_data',
        '//aios/table'
    ]
)
cc_test(
    name='ha3_sql_util_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    deps=[
        ':sql_ops_util', '//aios/table/table/test:table_testlib',
        '//aios/unittest_framework'
    ]
)
