package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_table_split_base',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    deps=[
        '//aios/autil:range_util', '//aios/ha3/ha3/sql/ops/util:sql_ops_util',
        '//aios/ha3/ha3/sql/data:sql_table_data'
    ],
    include_prefix='ha3/sql/ops/tableSplit',
    alwayslink=True
)
