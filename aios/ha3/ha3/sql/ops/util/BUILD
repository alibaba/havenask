package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_util',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    deps=[
        '//aios/matchdoc:matchdoc', '//aios/table:table',
        '//aios/ha3/ha3/sql/data:sql_data',
        '//aios/ha3/ha3/sql/resource:sql_resource',
        '//aios/ha3/ha3/sql/common:sql_common', '//aios/autil:object_tracer'
    ],
    include_prefix='ha3/sql/ops/util'
)
