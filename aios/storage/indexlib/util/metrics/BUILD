load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=[':__subpackages__'])
indexlib_cc_library(
    name='monitor',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':Monitor']
)
indexlib_cc_library(
    name='metric',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':Metric']
)
indexlib_cc_library(
    name='metric_reporter',
    srcs=[],
    hdrs=['MetricReporter.h', 'Statistic.h', 'TaggedMetricReporterGroup.h'],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':KmonitorTagvNormalizer', ':Monitor', '//aios/autil:env_util',
        '//aios/autil:metric', '//aios/autil:time',
        '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/util/counter'
    ]
)
indexlib_cc_library(
    name='progress_metrics',
    srcs=[],
    hdrs=['ProgressMetrics.h'],
    visibility=['//aios/storage/indexlib:__subpackages__']
)
indexlib_cc_library(
    name='KmonitorTagvNormalizer',
    deps=[
        '//aios/storage/indexlib/util:Bitmap',
        '//aios/storage/indexlib/util:Singleton'
    ]
)
indexlib_cc_library(
    name='IndexlibMetricControl',
    deps=[
        '//aios/autil:json', '//aios/autil:string_helper',
        '//aios/filesystem/fslib:fslib-framework',
        '//aios/storage/indexlib/util:Exception',
        '//aios/storage/indexlib/util:RegularExpression',
        '//aios/storage/indexlib/util:Singleton'
    ]
)
indexlib_cc_library(
    name='Metric',
    deps=[
        ':IndexlibMetricControl', '//aios/autil:log',
        '//aios/autil:string_helper', '//aios/autil:time',
        '//aios/kmonitor:kmonitor_client_cpp'
    ]
)
indexlib_cc_library(
    name='MetricProvider',
    deps=[
        ':Metric', '//aios/autil:lock', '//aios/autil:log',
        '//aios/kmonitor:kmonitor_client_cpp'
    ]
)
indexlib_cc_library(name='Monitor', srcs=[], deps=[':MetricProvider'])
