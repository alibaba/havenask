package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_plan_transform',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    deps=[
        '//aios/ha3/ha3/sql/ops/util:sql_ops_util',
        '//aios/ha3/ha3/sql/ops/planTransform:sql_graph_transform',
        '//aios/sql/iquan/cpp/common:iquan_common', '//navi:navi'
    ],
    include_prefix='ha3/sql/ops/planTransform',
    alwayslink=True
)
