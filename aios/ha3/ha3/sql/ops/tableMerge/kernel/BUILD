package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_table_merge',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='ha3/sql/ops/tableMerge/kernel',
    deps=[
        '//navi:navi', '//aios/ha3/ha3/sql/ops/util:sql_ops_util',
        '//aios/ha3/ha3/sql/data:sql_table_data'
    ],
    alwayslink=True
)
