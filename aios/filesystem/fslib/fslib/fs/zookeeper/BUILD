package(default_visibility=['//aios/filesystem/fslib:__subpackages__'])
load(
    '@rules_cc//examples:experimental_cc_shared_library.bzl',
    'cc_shared_library'
)
cc_shared_library(
    name='fslib_plugin_zfs',
    visibility=['//visibility:public'],
    roots=[':zfs'],
    preloaded_deps=[
        '//aios/autil:time', '//aios/autil:log', '//aios/autil:string_helper',
        '//aios/filesystem/fslib/fslib/fs:fs'
    ],
    static_deps=[
        '//third_party/zookeeper:zookeeper_import',
        '//third_party/zookeeper:zookeeper'
    ],
    exports_filter=[]
)
cc_library(
    name='zfs',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    deps=[
        '//aios/filesystem/fslib/fslib/fs:fs', '//aios/autil:time',
        '//aios/autil:log', '//aios/autil:string_helper',
        '//third_party/zookeeper:zookeeper'
    ],
    include_prefix='fslib/fs/zookeeper',
    alwayslink=True
)
cc_library(
    name='fslib_zookeeper_casehelper',
    visibility=['//visibility:public'],
    srcs=[
        'test/ZookeeperFsCaseHelper.cpp', 'test/FsCaseHelperFactory.cpp',
        'test/ZkServer.cpp'
    ],
    hdrs=[
        'test/ZkServer.h', 'test/FsCaseHelperBase.h',
        'test/FsCaseHelperFactory.h', 'test/ZookeeperFsCaseHelper.h'
    ],
    deps=[
        ':zfs', '//aios/filesystem/fslib:fslib-framework', '//aios/autil:time',
        '//aios/autil:net'
    ],
    data=['zookeeper_server'],
    include_prefix='fslib/fs/zookeeper',
    linkopts=[]
)
filegroup(
    name='zookeeper_server',
    srcs=glob(['test/testdata/**'], exclude=['test/testdata/zookeeper/var/**']),
    visibility=['//visibility:public']
)
