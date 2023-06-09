package(default_visibility=['//aios/ha3:__subpackages__'])
cc_library(
    name='sql_ops_scan_util',
    srcs=['ScanKernelUtil.cpp'],
    hdrs=['ScanKernelUtil.h'],
    include_prefix='ha3/sql/ops/scan',
    deps=[
        '//aios/ha3/ha3/sql/ops/scan:sql_ops_scan_base',
        '//aios/ha3/ha3/sql/ops/scan:sql_ops_scan_visitor',
        '//aios/ha3/ha3/sql/ops/scan:sql_ops_normal_scan',
        '//aios/ha3/ha3/sql/ops/tvf:sql_ops_tvf_wrapper',
        '//aios/ha3/ha3/sql/resource:sql_resource', '//navi:navi',
        '//navi_ops/coroutine:navi_coroutine_kernel'
    ]
)
cc_library(
    name='sql_ops_scan',
    hdrs=['ScanKernelBase.h', 'ScanKernel.h', 'AsyncScanKernel.h'],
    srcs=glob(['ScanKernelBase.cpp', 'ScanKernel.cpp', 'AsyncScanKernel.cpp']),
    deps=[
        ':sql_ops_scan_util',
        '//aios/ha3/ha3/sql/ops/externalTable/ha3sql:sql_ops_external_scan',
        '//aios/ha3/ha3/sql/proto:sql_proto'
    ],
    include_prefix='ha3/sql/ops/scan',
    alwayslink=True
)
