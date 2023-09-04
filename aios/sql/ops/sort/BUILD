package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_sort_init_param',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/ops/sort',
    deps=['//aios/sql/ops/calc:sql_ops_calc_table']
)
cc_test(
    name='test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    deps=[':sql_ops_sort_init_param', '//aios/unittest_framework']
)
