load('//bazel:defs.bzl', 'gen_conf_lib')
load('@pip//:requirements.bzl', 'requirement')
load('@rules_python//experimental/python:wheel.bzl', 'py_package', 'py_wheel')
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
    ]),
    hdrs=glob([
        'cpp_client/src/kmonitor/client/*.h',
        'cpp_client/src/kmonitor/client/sink/*.h',
        'cpp_client/src/kmonitor/client/core/*.h',
        'cpp_client/src/kmonitor/client/metric/*.h',
        'cpp_client/src/kmonitor/client/net/*.h',
        'cpp_client/src/kmonitor/client/net/thrift/*.h',
        'cpp_client/src/kmonitor/client/common/*.h'
    ]),
    copts=['-fno-strict-aliasing'],
    implementation_deps=['//aios/autil:regex'],
    strip_include_prefix='cpp_client/src',
    visibility=['//visibility:public'],
    deps=[
        '//aios/alog', '//aios/autil:data_buffer', '//aios/autil:json',
        '//aios/autil:metric', '//aios/autil:thread',
        '@com_google_googletest//:gtest_prod'
    ],
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
py_library(
    name='kmon_flume',
    srcs=glob(['python_client/flume/*.py']),
    visibility=['//visibility:public'],
    deps=[requirement('thrift')]
)
py_library(
    name='kmonitor_py',
    srcs=glob(['python_client/kmonitor/*.py',
               'python_client/kmonitor/**/*.py']),
    imports=['./python_client/'],
    visibility=['//visibility:public'],
    deps=[':kmon_flume']
)
py_package(
    name='kmonitor_py_pkg',
    visibility=['//visibility:public'],
    deps=[':kmon_flume', ':kmonitor_py']
)
py_wheel(
    name='kmonitor_whl',
    distribution='kmonitor',
    python_tag='py3',
    requires=['thrift'],
    strip_path_prefixes=['aios/kmonitor/python_client'],
    version='0.0.1',
    visibility=['//visibility:public'],
    deps=[':kmonitor_py_pkg'],
    tags=['manual']
)
py_test(
    name='kmon_py_test',
    srcs=glob(['python_client/test/*.py']),
    main='python_client/test/kmon_test.py',
    deps=[':kmonitor_py']
)
