package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_remote_scan',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h', '*.hpp']),
    include_prefix='ha3/sql/ops/remoteScan',
    deps=[
        '//aios/ha3:ha3_framework_headers',
        '//aios/ha3/ha3/sql/resource:sql_resource',
        '//aios/ha3/ha3/sql/common:sql_common',
        '//aios/ha3/ha3/sql/ops/util:sql_ops_util',
        '//aios/ha3/ha3/sql/ops/scan:sql_ops_scan_base',
        '//aios/suez:table_service_cc_proto',
        '//aios/suez:suez_kv_batch_get_generator'
    ],
    alwayslink=True
)
