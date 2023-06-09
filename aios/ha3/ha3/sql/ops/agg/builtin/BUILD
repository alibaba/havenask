package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_agg_builtin',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    deps=[
        '//aios/ha3/ha3/sql/ops/agg:sql_ops_agg_base',
        '//aios/ha3/ha3/sql/common:sql_log',
        '//aios/sql/iquan/cpp/common:iquan_common',
        '//aios/apps/facility/build_service:bs_plugin',
        '//aios/autil:plugin_base', '//aios/table:table'
    ],
    include_prefix='ha3/sql/ops/agg/builtin'
)
