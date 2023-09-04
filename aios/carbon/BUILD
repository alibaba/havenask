load('//bazel:defs.bzl', 'cc_proto')
load('//bazel:defs.bzl', 'gen_conf_lib')
carbon_config = gen_conf_lib(
    name='carbon_config',
    srcs=['src/config.h.in'],
    outs=['include/carbon/config.h'],
    args={
        '@CARBON_MASTER_VERSION@': '1.0.1',
        '@CARBON_BUILD_VERSION@': ''
    },
    strip_include_prefix='include'
)
carbon_cc_proto = cc_proto(
    name='carbon',
    srcs=glob(['carbon/proto/*.proto']),
    import_prefix='carbon',
    strip_import_prefix='carbon',
    deps=['//aios/hippo:proto', '//aios/network/arpc:proto']
)
cc_library(
    name='carbon_headers',
    hdrs=glob(['include/carbon/*.h', 'include/carbon/legacy/*.h']),
    strip_include_prefix='include',
    visibility=['//visibility:public']
)
cc_library(
    name='carbon',
    srcs=glob([
        'src/common/*.cpp', 'src/service/*.cpp', 'src/master/*.cpp',
        'src/compatible/*.cpp', 'src/common/*.h', 'src/service/*.h',
        'src/master/*.h', 'src/compatible/*.h', 'src/*.h'
    ]),
    includes=['src'],
    visibility=['//visibility:public'],
    deps=[
        ':carbon_headers', ':carbon_monitor', carbon_cc_proto, carbon_config,
        '//aios/apps/facility/cm2/cm_server/common:cm_server_base64',
        '//aios/apps/facility/cm2/cm_server/proto:cm_server_cc_proto',
        '//aios/autil:base64', '//aios/autil:closure_guard',
        '//aios/autil:lzma', '//aios/hippo', '//third_party/keycenter4c',
        '//third_party/lzma', '@curl', '//third_party/openssl:crypto'
    ]
)
cc_library(
    name='carbon_monitor',
    srcs=(
        glob([
            'src/monitor/CarbonMonitorSingleton.cpp',
            'src/monitor/CarbonMonitor.h',
            'src/monitor/CarbonMonitorSingleton.h', 'src/monitor/MonitorUtil.h',
            'src/common/common.h'
        ]) + ['src/monitor/CarbonKMonitor.cpp', 'src/monitor/CarbonKMonitor.h']
    ),
    includes=['src'],
    visibility=['//visibility:public'],
    deps=([
        ':carbon_headers', '//aios/alog', '//aios/autil:common_macros',
        '//aios/hippo'
    ] + [])
)
test_h = gen_conf_lib(
    name='test_h',
    srcs=['src/test/test.h.in'],
    outs=['src/test/test.h'],
    args={
        '@TOP_SRCDIR@': 'aios/carbon',
        '@TMP_TOP_SRCDIR@': '/tmp/aios/carbon'
    },
    strip_include_prefix='src'
)
test_data = glob(['src/testdata/*', 'src/testdata/**/*'])
