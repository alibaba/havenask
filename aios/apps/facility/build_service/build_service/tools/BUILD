load('@rules_cc//cc:defs.bzl', 'cc_binary', 'cc_library')
load('//bazel:defs.bzl', 'cc_proto')
cc_binary(
    name='bs_config_validator',
    srcs=glob(['config_validator/main.cpp']),
    visibility=['//visibility:public'],
    deps=['//aios/apps/facility/build_service:bs_admin', '//third_party/lua']
)
cc_library(
    name='remote_logger_lib',
    srcs=glob(['remote_logger/RemoteLogger.cpp']),
    hdrs=glob(['remote_logger/RemoteLogger.h']),
    include_prefix='build_service/tools/remote_logger',
    strip_include_prefix='remote_logger',
    visibility=['//visibility:public'],
    deps=[
        '//aios/apps/facility/build_service:bs_basic',
        '//aios/apps/facility/build_service:bs_config'
    ]
)
cc_binary(
    name='remote_logger_agent',
    srcs=glob(['remote_logger/main.cpp']),
    visibility=['//visibility:public'],
    deps=[':remote_logger_lib', '//aios/apps/facility/build_service:bs_util']
)
cc_binary(
    name='bs_generation_scanner',
    srcs=glob(['generation_scanner/main.cpp']),
    linkopts=[],
    visibility=['//visibility:public'],
    deps=['//aios/apps/facility/build_service:bs_util']
)
cc_proto(
    name='trace_server',
    srcs=['trace_server/TraceService.proto'],
    deps=['//aios/hippo:proto', '//aios/network/arpc:proto']
)
cc_binary(
    name='swift_doc_tracer',
    srcs=[
        'trace_server/SwiftDocTraceTool.cpp',
        'trace_server/TraceServerWorker.cpp', 'trace_server/TraceServerWorker.h'
    ],
    copts=['-Werror', '-fno-access-control'],
    visibility=['//visibility:public'],
    deps=[
        ':trace_server_cc_proto',
        '//aios/apps/facility/build_service/build_service/reader:bs_reader',
        '//aios/autil:closure_guard', '//aios/autil:log',
        '@com_google_absl//absl/flags:flag',
        '@com_google_absl//absl/flags:parse'
    ]
)
