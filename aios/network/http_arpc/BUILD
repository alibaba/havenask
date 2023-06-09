load('//bazel:defs.bzl', 'cc_proto', 'gen_conf_lib')
cc_library(
    name='http_arpc',
    srcs=glob([
        'HTTPRPCServer.cpp', 'HTTPRPCServerAdapter.cpp',
        'HTTPRPCServerWorkItem.cpp', 'HTTPRPCController.cpp',
        'HTTPRPCServerClosure.cpp', 'ProtoJsonizer.cpp', 'Log.cpp',
        'HTTPANetApp.cpp', '*.h'
    ]),
    hdrs=glob(['http_arpc/*.h']),
    deps=[
        '//aios/autil:zlib', '//aios/autil:string_helper', '//aios/autil:json',
        '//aios/autil:lock', '//aios/autil:url_util', '//aios/network/arpc:arpc'
    ],
    visibility=['//visibility:public']
)
gen_conf_lib(
    name='test_h',
    srcs=['test/test.h.in'],
    outs=['test/test.h'],
    args={
        '@TOP_SRCDIR@': 'aios/network/http_arpc',
        '@TOP_BUILDDIR@': '.',
        '@BUILDDIR@': '.',
        '@DOTEST_LOGGER_CONF@': '',
        '@INSTALLROOT@': '.'
    }
)
cc_proto(
    name='http_arpc_test_proto',
    srcs=['test/TestJsonize.proto', 'test/TestMessage.proto'],
    deps=['//aios/network/arpc:proto']
)
