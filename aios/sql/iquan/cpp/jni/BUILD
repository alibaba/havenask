package(default_visibility=['//aios/sql/iquan/cpp:__subpackages__'])
cc_library(
    name='iquan_jni',
    srcs=glob(['*.cpp', 'wrapper/*.cpp', 'jnipp/*.cpp']),
    hdrs=glob(['*.h', 'wrapper/*.h', 'jnipp/*.h']),
    include_prefix='iquan/jni',
    visibility=['//visibility:public'],
    deps=[
        '//aios/filesystem/fslib:fslib-framework', '//aios/protocol/access_log',
        '//aios/sql/iquan/cpp/common:iquan_common',
        '//aios/sql/iquan/cpp/common/catalog:iquan_catalog',
        '//aios/sql/iquan/cpp/config:iquan_config',
        '@bazel_tools//tools/jdk:jni', '@hdfs-cdh//:hdfs', '@jvm_libs//:libjvm'
    ]
)
filegroup(name='testdata', testonly=True, srcs=glob(['test/testdata/**']))
cc_library(
    name='iquan_jni_testlib',
    testonly=True,
    srcs=glob(['test/testlib/*.cpp']),
    hdrs=glob(['test/testlib/*.h']),
    include_prefix='iquan/jni',
    visibility=['//visibility:public'],
    deps=[':iquan_jni', '//aios/unittest_framework']
)
cc_test(
    name='iquan_jni_test',
    srcs=glob([
        'test/*Test.cpp', 'jnipp/test/*Test.cpp', 'wrapper/test/*Test.cpp'
    ]),
    copts=['-fno-access-control'],
    data=[
        ':testdata', '//aios/filesystem/fslib:fslib_plugin_hdfs',
        '//aios/sql/iquan/cpp:leak_suppression',
        '//aios/sql/iquan/cpp:test_alog_conf',
        '//aios/sql/iquan/java:iquan_client_jar'
    ],
    flaky=True,
    deps=[
        ':iquan_jni_testlib', '//aios/autil/autil/legacy/test:json_testlib',
        '//aios/unittest_framework'
    ]
)
cc_test(
    name='iquan_jni_itest',
    srcs=glob(['test/itest/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[
        '//aios/filesystem/fslib:fslib_plugin_hdfs',
        '//aios/sql/iquan/cpp:test_alog_conf',
        '//aios/sql/iquan/java:iquan_client_jar',
        '//integration_test/sql/sql-suites'
    ],
    flaky=True,
    deps=[':iquan_jni_testlib', '//aios/unittest_framework']
)
cc_library(
    name='iquan_jni_perf_testlib',
    testonly=True,
    srcs=glob(['test/ptest/IquanPerfTestBase.cpp']),
    hdrs=glob(['test/ptest/IquanPerfTestBase.h']),
    copts=['-fno-access-control'],
    include_prefix='iquan/jni',
    visibility=['//visibility:public'],
    deps=[':iquan_jni_testlib']
)
cc_test(
    name='iquan_jni_ptest',
    srcs=glob(['test/ptest/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[
        ':testdata', '//aios/filesystem/fslib:fslib_plugin_hdfs',
        '//aios/sql/iquan/cpp:leak_suppression',
        '//aios/sql/iquan/cpp:test_alog_conf',
        '//aios/sql/iquan/java:iquan_client_jar'
    ],
    flaky=True,
    deps=[':iquan_jni_perf_testlib', '//aios/unittest_framework']
)
cc_test(
    name='iquan_benchmark',
    srcs=glob(['test/benchmark/*Benchmark.cpp']),
    copts=['-fno-access-control'],
    data=[],
    linkopts=[],
    deps=[':iquan_jni', '//aios/unittest_framework:unittest_benchmark']
)
