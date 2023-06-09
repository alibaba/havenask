package(default_visibility=['//visibility:private'])
load('//bazel:defs.bzl', 'cc_proto', 'gen_conf_lib')
cc_library(
    name='worker_framework_headers',
    hdrs=glob(['include/worker_framework/*.h']),
    copts=[],
    strip_include_prefix='include',
    visibility=['//visibility:public'],
    deps=[
        '//aios/autil:compression', '//aios/autil:option_parser',
        '//aios/autil:string_helper', '//aios/autil:thread'
    ]
)
cc_library(
    name='worker_framework',
    srcs=glob([
        'src/common/*.h', 'src/common/*.cpp',
        'src/data_client/SimpleDataClient.cpp',
        'src/data_client/DataClientWrapper.cpp', 'src/data_client/DataItem.cpp',
        'src/worker_base/*.h', 'src/worker_base/*.cpp'
    ],
              exclude=[
                  'src/worker_base/LeaderLocator.cpp', 'src/common/PathUtil.cpp'
              ]),
    copts=[],
    includes=['src'],
    strip_include_prefix='include',
    visibility=['//visibility:public'],
    deps=([
        ':leader_locator', ':worker_framework_headers', '@cm2//:cm2',
        '//aios/autil:compression', '//aios/autil:murmur_hash',
        '//aios/autil:net', '//aios/autil:option_parser',
        '//aios/autil:string_helper', '//aios/autil:thread',
        '//aios/autil:time', '//aios/filesystem/fslib:fslib-framework',
        '//aios/network/arpc', '//aios/network/http_arpc'
    ] + [])
)
cc_library(
    name='leader_locator',
    srcs=glob(['src/worker_base/LeaderLocator.cpp', 'src/common/PathUtil.cpp']),
    hdrs=glob([
        'include/worker_framework/PathUtil.h',
        'include/worker_framework/LeaderLocator.h'
    ]),
    includes=['src'],
    strip_include_prefix='include',
    visibility=['//visibility:public'],
    deps=['@cm2//:cm2', '//aios/filesystem/fslib:fslib-framework']
)
gen_conf_lib(
    name='test_header',
    srcs=['src/test/test.h.in'],
    outs=['src/test/test.h'],
    args={
        '@TOP_SRCDIR@': 'aios/worker_framework',
        '@TOP_BUILDDIR@': '.',
        '@BUILDDIR@': '.',
        '@DOTEST_LOGGER_CONF@': '.',
        '@INSTALLROOT@': '.'
    },
    strip_include_prefix='src'
)
cc_proto(
    name='test_proto',
    srcs=['src/worker_base/test/TestMessage.proto'],
    deps=['//aios/network/arpc:proto']
)
