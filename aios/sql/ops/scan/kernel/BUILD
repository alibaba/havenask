package(default_visibility=['//aios/sql:__subpackages__'])
cc_library(
    name='sql_ops_scan',
    srcs=glob([
        'ScanKernel.cpp', 'AsyncScanKernel.cpp', 'LogicalScanKernel.cpp',
        'ExternalScanKernel.cpp'
    ]),
    hdrs=[
        'AsyncScanKernel.h', 'ExternalScanKernel.h', 'LogicalScanKernel.h',
        'ScanKernel.h'
    ],
    include_prefix='sql/ops/scan',
    deps=[
        '//aios/sql/ops/externalTable/ha3sql:sql_ops_external_scan',
        '//aios/sql/ops/scan:sql_ops_normal_scan',
        '//aios/sql/ops/scan:sql_ops_scan_base',
        '//aios/sql/ops/tvf:sql_ops_tvf_wrapper', '//aios/sql/proto:sql_proto'
    ],
    alwayslink=True
)
cc_test(
    name='test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[],
    linkopts=['-Wl,--as-needed'],
    shard_count=3,
    deps=[
        ':sql_ops_scan',
        '//aios/ha3/ha3_sdk/testlib/index:ha3_sdk_testlib_index',
        '//aios/sql/ops/scan:fake_tokenizer', '//aios/sql/ops/test:ops_testlib',
        '//aios/storage/indexlib/table/kv_table/test:kv_table_test_helper',
        '//aios/unittest_framework'
    ]
)
