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
    name='simple_data_client',
    srcs=[
        'src/data_client/SimpleDataClient.cpp',
        'src/data_client/DataClientWrapper.cpp', 'src/data_client/DataItem.cpp'
    ],
    includes=['src'],
    strip_include_prefix='include',
    deps=[
        ':leader_locator', ':worker_framework_headers', '//aios/autil:time',
        '//aios/autil:string_helper', '//aios/filesystem/fslib:fslib-framework'
    ]
)
cc_library(
    name='worker_framework',
    srcs=glob([
        'src/common/*.h', 'src/common/*.cpp', 'src/worker_base/*.h',
        'src/worker_base/*.cpp'
    ],
              exclude=[
                  'src/worker_base/LeaderLocator.cpp', 'src/common/PathUtil.cpp'
              ]),
    copts=[],
    includes=['src'],
    strip_include_prefix='include',
    visibility=['//visibility:public'],
    deps=([
        ':leader_locator', ':worker_framework_headers',
        '//aios/apps/facility/cm2/cm_basic', '//aios/autil:compression',
        '//aios/autil:murmur_hash', '//aios/autil:net',
        '//aios/autil:option_parser', '//aios/autil:string_helper',
        '//aios/autil:thread', '//aios/autil:time',
        '//aios/filesystem/fslib:fslib-framework', '//aios/network/arpc',
        '//aios/network/http_arpc'
    ] + ['simple_data_client'])
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
    deps=[
        '//aios/apps/facility/cm2/cm_basic',
        '//aios/filesystem/fslib:fslib-framework'
    ]
)
cc_library(
    name='worker_base_main',
    srcs=glob(['src/main/main.cpp']),
    hdrs=[],
    copts=[],
    visibility=['//visibility:public'],
    deps=[':worker_framework']
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
cc_test(
    name='simple_data_client_test',
    srcs=['src/data_client/test/SimpleDataClientTest.cpp'],
    copts=['-Iaios/worker_framework', '-fno-access-control'],
    data=glob(['src/testdata/**']),
    flaky=True,
    linkopts=[],
    deps=[':test_header', ':simple_data_client', '//aios/unittest_framework']
)
cc_test(
    name='worker_framework_test',
    srcs=glob([
        'src/common/test/*.cpp', 'src/worker_base/test/*.h',
        'src/worker_base/test/*.cpp'
    ],
              exclude=['src/worker_base/test/LeaderCheckerTest.cpp']),
    copts=['-Iaios/worker_framework', '-fno-access-control'],
    data=glob(['src/testdata/**']),
    flaky=True,
    linkopts=[],
    deps=[
        ':test_header', ':test_proto_cc_proto', ':worker_framework',
        '//aios/filesystem/fslib/fslib/fs/zookeeper:fslib_zookeeper_casehelper',
        '//aios/unittest_framework', '@com_google_googletest//:gtest_main'
    ]
)
cc_proto(
    name='test_proto',
    srcs=['src/worker_base/test/TestMessage.proto'],
    deps=['//aios/network/arpc:proto']
)
