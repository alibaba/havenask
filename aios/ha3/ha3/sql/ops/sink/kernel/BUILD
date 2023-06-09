package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_sink',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='ha3/sql/ops/sink',
    deps=[
        '//aios/ha3/ha3/sql/ops/util:sql_ops_util',
        '//aios/ha3/ha3/sql/resource:sql_resource', '//navi:navi',
        '//aios/table:table'
    ],
    alwayslink=True
)
