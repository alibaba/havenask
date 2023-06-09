package(default_visibility=['//visibility:private'])
load('//bazel:defs.bzl', 'gen_conf_lib')
load(
    '@rules_cc//examples:experimental_cc_shared_library.bzl',
    'cc_shared_library'
)
cc_shared_library(
    name='fslib_plugin_hdfs',
    visibility=['//visibility:public'],
    roots=[':hdfs'],
    preloaded_deps=[
        '//aios/autil:log', '//aios/autil:lock', '//aios/autil:string_helper',
        '//aios/filesystem/fslib:fslib-framework'
    ],
    static_deps=['@hdfs-cdh//:hdfs', '@hdfs-cdh//:hdfs_headers']
)
cc_library(
    name='hdfs',
    visibility=['//visibility:public'],
    srcs=glob(['*.cpp']),
    hdrs=glob(['fslib/*.h', '*.h']),
    deps=[
        '//aios/filesystem/fslib:fslib-framework', '//aios/autil:log',
        '//aios/autil:lock', '//aios/autil:string_helper', '@hdfs-cdh//:hdfs'
    ],
    alwayslink=True,
    include_prefix='fslib/fs/hdfs'
)
