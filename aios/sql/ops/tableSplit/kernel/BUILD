package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_table_split',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/ops/tableSplit',
    deps=['//aios/navi', '//aios/sql/ops/tableSplit:sql_ops_table_split_base'],
    alwayslink=True
)
