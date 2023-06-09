package(default_visibility=['//aios/ha3:__subpackages__'])
load('//bazel:defs.bzl', 'if_clang')
cc_library(
    name='sql_orc_utils',
    hdrs=['RowGroupToTableConverter.h'],
    srcs=['RowGroupToTableConverter.cpp'],
    deps=[
        '//aios/storage/indexlib/table/orc_table:reader', '//aios/table:table'
    ],
    include_prefix='ha3/sql/ops/orcScan',
    alwayslink=True
)
cc_library(
    name='sql_ops_orc_scan',
    hdrs=(['ScanResult.h'] +
          if_clang(['AsyncOrcScan.h', 'AsyncOrcScanCallbackCtx.h'])),
    srcs=([] + if_clang(['AsyncOrcScan.cpp', 'AsyncOrcScanCallbackCtx.cpp'])),
    copts=if_clang(['-std=c++20']),
    deps=['//aios/ha3/ha3/sql/ops/scan/kernel:sql_ops_scan_util'],
    implementation_deps=[
        ':sql_orc_utils', '//aios/storage/indexlib/table/orc_table:table'
    ],
    include_prefix='ha3/sql/ops/orcScan',
    alwayslink=True
)
cc_library(
    name='sql_ops_orc_scan_kernel',
    srcs=['TabletOrcScanKernel.cpp'],
    copts=if_clang(['-std=c++20']),
    deps=[
        ':sql_ops_orc_scan', '//aios/ha3/ha3/sql/data:sql_data',
        '//aios/ha3/ha3/sql/resource:sql_resource'
    ],
    implementation_deps=['//aios/storage/indexlib/table/orc_table:reader'],
    alwayslink=True
)
