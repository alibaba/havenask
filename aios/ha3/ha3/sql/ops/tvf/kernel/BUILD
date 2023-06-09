package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_tvf',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='ha3/sql/ops/tvf',
    deps=[
        '//aios/ha3/ha3/sql/proto:sql_proto',
        '//aios/ha3/ha3/sql/resource:sql_resource',
        '//aios/ha3/ha3/sql/ops/tvf:sql_ops_tvf_wrapper', '//navi:navi'
    ],
    alwayslink=True
)
