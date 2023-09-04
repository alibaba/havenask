cc_library(
    name='arpc_metric',
    srcs=[
        'MetricReporterConfig.cpp', 'ClientRPCStats.cpp',
        'ClientMetricReporter.cpp', 'ServerRPCStats.cpp',
        'ServerMetricReporter.cpp'
    ],
    hdrs=[
        'MetricReporterConfig.h', 'ClientRPCStats.h', 'ClientMetricReporter.h',
        'ServerRPCStats.h', 'ServerMetricReporter.h'
    ],
    deps=[
        '//aios/autil:log', '//aios/autil:json', '//aios/autil:time',
        '//aios/autil:env_util', '//aios/network/arpc:proto_cc_proto'
    ],
    visibility=['//visibility:public']
)
