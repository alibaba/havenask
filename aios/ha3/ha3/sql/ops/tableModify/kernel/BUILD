package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_table_modify',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='ha3/sql/ops/tableModify/kernel',
    deps=[
        '//navi:navi', '//aios/ha3/ha3/sql/ops/calc:sql_ops_calc_table',
        '//aios/ha3/ha3/sql/ops/util:sql_ops_util',
        '//aios/ha3/ha3/sql/ops/tableModify:sql_ops_table_modify_base',
        '//aios/ha3/ha3/sql/resource:sql_resource', '//aios/autil:json_helper'
    ],
    alwayslink=True
)
