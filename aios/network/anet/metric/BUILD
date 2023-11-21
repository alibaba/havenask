cc_library(
    name='anet_metric',
    srcs=['ANetMetricReporter.cpp'],
    hdrs=['ANetMetricReporterConfig.h', 'ANetMetricReporter.h'],
    deps=[
        '//aios/network/anet:anet', '//aios/autil:env_util',
        '//aios/autil:json', '//aios/autil:thread'
    ],
    visibility=['//visibility:public']
)
cc_library(
    name='kmonitor_anet_metric',
    srcs=['KMonitorANetMetricReporter.cpp'],
    hdrs=['KMonitorANetMetricReporter.h'],
    deps=[':anet_metric', '//aios/kmonitor:kmonitor_client_cpp'],
    visibility=['//visibility:public']
)
