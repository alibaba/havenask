package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_table_modify_base',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='ha3/sql/ops/tableModify',
    deps=[
        '//navi:navi', '//aios/ha3/ha3/sql/ops/util:sql_ops_util',
        '//aios/ha3/ha3/sql/ops/condition:sql_ops_condition',
        '//aios/ha3/ha3/sql/data:sql_table_data', '//aios/autil:json_helper'
    ],
    alwayslink=True
)
