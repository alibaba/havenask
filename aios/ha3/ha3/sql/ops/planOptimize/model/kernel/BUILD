package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_plan_optimize',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='ha3/sql/ops/planOptimize/model',
    deps=[
        '//aios/ha3/ha3/sql/ops/planOptimize/model:sql_model_optimizer',
        '//aios/ha3/ha3/sql/ops/util:sql_ops_util', '//navi'
    ],
    alwayslink=True
)
