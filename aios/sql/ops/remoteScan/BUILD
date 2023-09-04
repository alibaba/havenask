package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_remote_scan',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h', '*.hpp']),
    include_prefix='sql/ops/remoteScan',
    deps=[
        '//aios/sql/common:sql_common', '//aios/sql/ops/scan:sql_ops_scan_base',
        '//aios/sql/ops/util:sql_ops_util', '//aios/sql/resource:sql_resource',
        '//aios/suez/service:suez_kv_batch_get_generator',
        '//aios/suez/service:table_service_cc_proto'
    ],
    alwayslink=True
)
cc_test(
    name='sql_ops_remote_scan_test',
    srcs=glob(['test/TableServiceConnectorTest.cpp']),
    copts=['-fno-access-control'],
    linkopts=['-Wl,--as-needed'],
    deps=[
        ':sql_ops_remote_scan', '//aios/table/table/test:table_testlib',
        '//aios/unittest_framework'
    ]
)
