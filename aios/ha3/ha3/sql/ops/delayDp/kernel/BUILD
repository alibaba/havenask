package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_delay_dp',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    deps=[
        '//aios/ha3/ha3/sql/ops/tableSplit:sql_ops_table_split_base',
        '//aios/ha3/ha3/sql/ops/runSqlGraph:sql_ops_run_sql_graph',
        '//aios/ha3/ha3/sql/ops/util:sql_ops_util', '//navi:navi',
        '//aios/ha3/ha3/sql/resource:sql_resource',
        '//aios/ha3/ha3/sql/proto:sql_proto', '//aios/table:table'
    ],
    include_prefix='ha3/sql/ops/delayDp',
    alwayslink=True
)
