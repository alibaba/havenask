package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_external_scan',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='sql/ops/externalTable/ha3sql',
    deps=[
        '//aios/ha3:ha3_proto', '//aios/sql/common:sql_common',
        '//aios/sql/framework:sql_framework',
        '//aios/sql/ops/externalTable:sql_ops_external_gig',
        '//aios/sql/ops/scan:sql_ops_scan_base',
        '//aios/sql/ops/util:sql_ops_util', '//aios/sql/resource:sql_resource'
    ],
    alwayslink=True
)
cc_test(
    name='test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    linkopts=['-Wl,--as-needed'],
    deps=[
        ':sql_ops_external_scan', '//aios/navi:navi_log_testlib',
        '//aios/navi:navi_ops_testlib', '//aios/table/test:table_testlib',
        '//aios/unittest_framework'
    ]
)
