package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_external_scan',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='ha3/sql/ops/externalTable/ha3sql',
    deps=[
        '//aios/ha3/ha3/sql/resource:sql_resource',
        '//aios/ha3/ha3/sql/common:sql_common',
        '//aios/ha3/ha3/sql/ops/util:sql_ops_util',
        '//aios/ha3/ha3/sql/ops/scan:sql_ops_scan_base', '//aios/ha3:ha3_proto',
        '//aios/ha3/ha3/sql/ops/externalTable:sql_ops_external_gig',
        '//aios/ha3/ha3/sql/framework:sql_framework'
    ],
    alwayslink=True
)
