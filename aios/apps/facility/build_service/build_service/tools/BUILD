load('@rules_cc//cc:defs.bzl', 'cc_binary', 'cc_library')
load('//bazel:defs.bzl', 'cc_proto')
load('//bazel:bundle.bzl', 'bundle_files', 'bundle_tar')
cc_binary(
    name='bs_generation_scanner',
    srcs=glob(['generation_scanner/main.cpp']),
    linkopts=[],
    visibility=['//visibility:public'],
    deps=['//aios/apps/facility/build_service:bs_util']
)
cc_proto(
    name='rt_worker',
    srcs=glob(['mini_rt_server/*.proto']),
    import_prefix='mini_rt_server',
    strip_import_prefix='mini_rt_server',
    visibility=['//visibility:public'],
    deps=[
        '//aios/apps/facility/build_service:proto', '//aios/hippo:proto',
        '//aios/network/arpc:proto'
    ]
)
cc_proto(
    name='trace_server',
    srcs=['trace_server/TraceService.proto'],
    deps=['//aios/hippo:proto', '//aios/network/arpc:proto']
)
