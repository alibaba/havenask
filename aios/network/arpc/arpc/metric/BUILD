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
cc_library(
    name='kmonitor_arpc_metric',
    srcs=[
        'KMonitorClientMetricReporter.cpp', 'KMonitorServerMetricReporter.cpp'
    ],
    hdrs=['KMonitorClientMetricReporter.h', 'KMonitorServerMetricReporter.h'],
    deps=[':arpc_metric', '//aios/kmonitor:kmonitor_client_cpp'],
    visibility=['//visibility:public']
)
cc_library(
    name='kmonitor_anet_metric',
    srcs=[
        'KMonitorANetClientMetricReporter.cpp',
        'KMonitorANetServerMetricReporter.cpp'
    ],
    hdrs=[
        'KMonitorANetMetricReporterConfig.h',
        'KMonitorANetClientMetricReporter.h',
        'KMonitorANetServerMetricReporter.h'
    ],
    deps=[
        ':kmonitor_arpc_metric',
        '//aios/network/anet/metric:kmonitor_anet_metric'
    ],
    visibility=['//visibility:public']
)
