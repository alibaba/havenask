package(default_visibility=['//visibility:private'])
cc_library(
    name='unittest_framework',
    visibility=['//visibility:public'],
    srcs=glob(['unittest/dotest.cpp']),
    deps=[
        '@com_google_protobuf//:protobuf', ':unittest_lib', '//aios/alog:alog',
        '//aios/autil:backtrace', '//aios/autil:env_util'
    ]
)
cc_library(
    name='unittest_benchmark',
    visibility=['//visibility:public'],
    srcs=glob(['unittest/benchmark.cpp']),
    deps=[
        ':unittest_lib', '@com_google_benchmark//:benchmark', '//aios/autil:log'
    ]
)
cc_library(
    name='unittest_lib',
    visibility=['//visibility:public'],
    hdrs=glob(['unittest/*.h']),
    include_prefix='unittest',
    strip_include_prefix='unittest',
    deps=['@com_google_googletest//:gtest']
)
