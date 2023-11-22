package(
    default_visibility=[
        '//aios/sql:__subpackages__',
        '//aios/apps/engine/vector_service:__subpackages__'
    ]
)
cc_library(
    name='sql_ops_sort',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/ops/sort',
    deps=[
        '//aios/navi', '//aios/sql/ops/sort:sql_ops_sort_init_param',
        '//aios/sql/ops/util:sql_ops_util', '//aios/sql/proto:sql_proto',
        '//aios/sql/resource:sql_resource'
    ],
    alwayslink=True
)
cc_test(
    name='ha3_sql_sort_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[],
    deps=[
        ':sql_ops_sort', '//aios/sql/ops/test:ops_testlib',
        '//aios/unittest_framework'
    ]
)
