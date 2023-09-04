package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_table_split_base',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/ops/tableSplit',
    deps=[
        '//aios/autil:range_util', '//aios/sql/data:sql_table_data',
        '//aios/sql/ops/util:sql_ops_util'
    ],
    alwayslink=True
)
cc_test(
    name='test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    linkopts=['-Wl,--as-needed'],
    deps=[
        ':sql_ops_table_split_base', '//aios/table/table/test:table_testlib',
        '//aios/unittest_framework'
    ]
)
