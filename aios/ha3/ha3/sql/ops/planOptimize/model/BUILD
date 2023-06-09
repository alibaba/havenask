package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_model_optimizer',
    hdrs=['SqlModelGraphOptimizer.h'],
    srcs=glob(['SqlModelGraphOptimizer.cpp']),
    deps=[
        '//aios/ha3:ha3_model_handler', '//aios/sql/iquan/cpp/jni:iquan_jni',
        '//aios/ha3/ha3/sql/common:sql_common',
        '//aios/ha3/ha3/sql/ops/planOptimize:sql_graph_optimize_base'
    ],
    copts=['-Werror'],
    visibility=['//visibility:public'],
    include_prefix='ha3/sql/ops/planOptimize/model'
)
