package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_delay_dp',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/ops/delayDp',
    deps=[
        '//aios/navi', '//aios/sql/ops/runSqlGraph:sql_ops_run_sql_graph',
        '//aios/sql/ops/tableSplit:sql_ops_table_split_base',
        '//aios/sql/ops/util:sql_ops_util', '//aios/sql/proto:sql_proto',
        '//aios/sql/resource:sql_resource', '//aios/table'
    ],
    alwayslink=True
)
