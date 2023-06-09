package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_calc_table',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    deps=[
        '//aios/ha3/ha3/sql/ops/condition:sql_ops_condition',
        '//aios/ha3/ha3/sql/proto:sql_proto',
        '//aios/ha3/ha3/sql/ops/util:sql_ops_util',
        '//aios/ha3/ha3/sql/framework:sql_push_down_op', '//navi:navi',
        '//aios/autil:plugin_base', '//aios/table:table'
    ],
    include_prefix='ha3/sql/ops/calc'
)
