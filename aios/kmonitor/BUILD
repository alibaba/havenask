load('//bazel:defs.bzl', 'gen_conf_lib')
cc_library(
    name='kmonitor_client_cpp',
    srcs=glob([
        'cpp_client/src/kmonitor/client/*.cpp',
        'cpp_client/src/kmonitor/client/common/*.cpp',
        'cpp_client/src/kmonitor/client/core/*.cpp',
        'cpp_client/src/kmonitor/client/metric/*.cpp',
        'cpp_client/src/kmonitor/client/net/*.cpp',
        'cpp_client/src/kmonitor/client/net/thrift/*.cpp',
        'cpp_client/src/kmonitor/client/sink/*.cpp'
    ],
              exclude=['cpp_client/src/kmonitor/client/MetricsReporterR.cpp']),
    hdrs=glob([
        'cpp_client/src/kmonitor/client/*.h',
        'cpp_client/src/kmonitor/client/sink/*.h',
        'cpp_client/src/kmonitor/client/core/*.h',
        'cpp_client/src/kmonitor/client/metric/*.h',
        'cpp_client/src/kmonitor/client/net/*.h',
        'cpp_client/src/kmonitor/client/net/thrift/*.h',
        'cpp_client/src/kmonitor/client/common/*.h'
    ],
              exclude=['cpp_client/src/kmonitor/client/MetricsReporterR.h']),
    copts=['-fno-strict-aliasing'],
    implementation_deps=['//aios/autil:regex'],
    strip_include_prefix='cpp_client/src',
    visibility=['//visibility:public'],
    deps=[
        '//aios/alog', '//aios/autil:data_buffer', '//aios/autil:env_util', '//aios/autil:net',
        '//aios/autil:json', '//aios/autil:metric', '//aios/autil:thread',
        '//aios/network/curl_client:curl_client_lib',
        '@com_google_googletest//:gtest_prod'
    ],
    alwayslink=1
)
cc_library(
    name='metrics_report_navi_resource',
    srcs=['cpp_client/src/kmonitor/client/MetricsReporterR.cpp'],
    hdrs=['cpp_client/src/kmonitor/client/MetricsReporterR.h'],
    strip_include_prefix='cpp_client/src',
    visibility=['//visibility:public'],
    deps=[':kmonitor_client_cpp', '//aios/navi'],
    alwayslink=1
)
gen_conf_lib(
    name='kmonitor_cpp_test_h',
    srcs=['cpp_client/src/kmonitor/test/test.h.in'],
    outs=['cpp_client/src/kmonitor/test/test.h'],
    args={'@TEST_DATA_PATH@': 'aios/kmonitor/cpp_client/testdata'},
    include_prefix='cpp_client/src',
    strip_include_prefix='cpp_client/src'
)
cc_library(
    name='kmonitor_cpp_adapter',
    srcs=glob(['adapter/kmonitor_adapter/*.cpp']),
    hdrs=glob(['adapter/kmonitor_adapter/*.h']),
    include_prefix='kmonitor_adapter',
    strip_include_prefix='adapter/kmonitor_adapter',
    visibility=['//visibility:public'],
    deps=[
        ':kmonitor_client_cpp', '//aios/alog', '//aios/autil:env_util',
        '//aios/autil:scope', '//aios/autil:thread'
    ],
    alwayslink=1
)
