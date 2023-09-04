package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_rpc',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/rpc',
    deps=[
        '//aios/ha3:ha3_proto_headers', '//aios/ha3:ha3_proto_rpc_cc_proto',
        '//aios/navi', '//aios/sql/data:sql_data',
        '//aios/sql/framework:sql_framework',
        '//aios/sql/ops/metaCollect:sql_meta_collector',
        '//aios/sql/resource:sql_resource'
    ],
    alwayslink=True
)
