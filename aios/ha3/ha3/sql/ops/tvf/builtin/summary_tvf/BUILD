package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_tvf_summary',
    srcs=glob(['*.cpp'], exclude=['OneSummaryTvfFunc.cpp']),
    hdrs=glob(['*.h'], exclude=['OneSummaryTvfFunc.h']),
    include_prefix='ha3/sql/ops/tvf/builtin',
    deps=[
        '//aios/ha3/ha3/sql/ops/tvf/builtin:sql_ops_tvf_builtin',
        '//aios/ha3/ha3/sql/ops/condition:sql_ops_condition',
        '//aios/ha3/ha3/sql/ops/util:sql_ops_util',
        '//aios/ha3:ha3_framework_headers', '//aios/ha3/ha3/common:ha3_common'
    ]
)
