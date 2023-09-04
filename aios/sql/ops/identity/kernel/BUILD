package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_identity',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/ops/identity',
    deps=[
        '//aios/navi', '//aios/sql/ops/util:sql_ops_util',
        '//aios/sql/proto:sql_proto', '//aios/sql/resource:sql_resource'
    ],
    alwayslink=True
)
