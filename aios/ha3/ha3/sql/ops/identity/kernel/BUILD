package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_identity',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    deps=[
        '//aios/ha3/ha3/sql/ops/util:sql_ops_util',
        '//aios/ha3/ha3/sql/resource:sql_resource',
        '//aios/ha3/ha3/sql/proto:sql_proto', '//navi:navi'
    ],
    include_prefix='ha3/sql/ops/identity',
    alwayslink=True
)
