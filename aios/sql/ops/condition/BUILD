package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_condition',
    srcs=glob(['*.cpp'], exclude=['ExprGenerateVisitor2.cpp']),
    hdrs=glob(['*.h'], exclude=['ExprGenerateVisitor2.cpp']),
    include_prefix='sql/ops/condition',
    visibility=['//visibility:public'],
    deps=([
        '//aios/autil:json_helper', '//aios/sql/common:sql_common',
        '//aios/suez_turing/suez/turing/expression:turing_expression'
    ] + [])
)
cc_test(
    name='ha3_sql_condition_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[],
    deps=[
        ':sql_ops_condition',
        '//aios/storage/indexlib/indexlib:indexlib_testlib',
        '//aios/unittest_framework'
    ]
)
