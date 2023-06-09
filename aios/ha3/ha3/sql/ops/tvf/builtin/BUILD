package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_tvf_builtin',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='ha3/sql/ops/tvf/builtin',
    deps=[
        '//aios/ha3/ha3/sql/ops/tvf:sql_ops_tvf_base',
        '//aios/sql/iquan/cpp/common:iquan_common',
        '//aios/apps/facility/build_service:bs_plugin', '//aios/table:table'
    ]
)
