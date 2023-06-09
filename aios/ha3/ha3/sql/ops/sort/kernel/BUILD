package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_sort',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='ha3/sql/ops/sort',
    deps=[
        '//aios/ha3/ha3/sql/ops/util:sql_ops_util',
        '//aios/ha3/ha3/sql/ops/sort:sql_ops_sort_init_param',
        '//aios/ha3/ha3/sql/resource:sql_resource',
        '//aios/ha3/ha3/sql/proto:sql_proto', '//navi:navi'
    ],
    alwayslink=True
)
