package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_agg',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='ha3/sql/ops/agg',
    deps=[
        '//aios/ha3/ha3/sql/ops/agg:sql_ops_agg_base',
        '//aios/ha3/ha3/sql/ops/agg:sql_ops_agg_fuc_manager',
        '//aios/ha3/ha3/sql/ops/agg:sql_ops_agg_op',
        '//aios/ha3/ha3/sql/ops/calc:sql_ops_calc_table', '//navi:navi'
    ],
    alwayslink=True
)
