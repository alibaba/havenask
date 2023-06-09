package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_run_sql_graph',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    deps=[
        '//aios/ha3/ha3/sql/ops/util:sql_ops_util', '//navi:navi',
        '//aios/autil:range_util'
    ],
    include_prefix='ha3/sql/ops/runSqlGraph',
    alwayslink=True
)
