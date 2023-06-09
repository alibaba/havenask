package(default_visibility=['//aios/sql/iquan/cpp:__subpackages__'])
load('//bazel:defs.bzl', 'copy_target_to')
cc_library(
    name='iquan_jni',
    srcs=glob(['*.cpp', 'wrapper/*.cpp', 'jnipp/*.cpp']),
    hdrs=glob(['*.h', 'wrapper/*.h', 'jnipp/*.h']),
    include_prefix='iquan/jni',
    visibility=['//visibility:public'],
    deps=[
        '//aios/filesystem/fslib:fslib-framework',
        '//aios/sql/iquan/cpp/common:iquan_common',
        '//aios/sql/iquan/cpp/common/catalog:iquan_catalog',
        '//aios/sql/iquan/cpp/config:iquan_config',
        '@bazel_tools//tools/jdk:jni', '@hdfs-cdh//:hdfs', '@jvm_libs//:libjvm'
    ]
)
