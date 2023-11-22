package(default_visibility=['//visibility:private'])
load(
    '@rules_cc//examples:experimental_cc_shared_library.bzl',
    'cc_shared_library'
)
cc_shared_library(
    name='fslib_plugin_hdfs',
    preloaded_deps=[
        '//aios/autil:log', '//aios/autil:lock', '//aios/autil:string_helper',
        '//aios/filesystem/fslib:fslib-framework'
    ],
    roots=[':hdfs'],
    static_deps=['@hdfs-cdh//:hdfs', '@hdfs-cdh//:hdfs_headers'],
    visibility=['//visibility:public']
)
cc_library(
    name='hdfs',
    srcs=glob(['*.cpp']),
    hdrs=glob(['fslib/*.h', '*.h']),
    include_prefix='fslib/fs/hdfs',
    visibility=['//visibility:public'],
    deps=[
        '//aios/autil:lock', '//aios/autil:log', '//aios/autil:string_helper',
        '//aios/filesystem/fslib:fslib-framework', '@hdfs-cdh//:hdfs'
    ],
    alwayslink=True
)
