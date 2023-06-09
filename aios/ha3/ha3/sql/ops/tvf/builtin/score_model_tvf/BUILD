package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_tvf_score_model',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='ha3/sql/ops/tvf/builtin',
    deps=[
        '//aios/ha3/ha3/sql/ops/tvf/builtin:sql_ops_tvf_builtin',
        '//aios/basic_ops:qinfo_variant', '//aios/basic_ops:basic_ops_op_util',
        '@suez_turing.turbojet.turing_ops_util//:suez_turing',
        '//aios/ha3/ha3/sql/resource:sql_resource',
        '//aios/ha3/ha3/common:common_def',
        '//aios/ha3/ha3/common/access_log:access_log',
        '//aios/ha3:ha3_model_handler'
    ]
)
