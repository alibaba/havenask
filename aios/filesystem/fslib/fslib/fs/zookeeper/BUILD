package(default_visibility=['//aios/filesystem/fslib:__subpackages__'])
load(
    '@rules_cc//examples:experimental_cc_shared_library.bzl',
    'cc_shared_library'
)
cc_shared_library(
    name='fslib_plugin_zfs',
    exports_filter=[],
    preloaded_deps=[
        '//aios/autil:time', '//aios/autil:log', '//aios/autil:string_helper',
        '//aios/filesystem/fslib/fslib/fs:fs'
    ],
    roots=[':zfs'],
    static_deps=[
        '//third_party/zookeeper:zookeeper_import',
        '//third_party/zookeeper:zookeeper'
    ],
    visibility=['//visibility:public']
)
cc_library(
    name='zfs',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='fslib/fs/zookeeper',
    deps=[
        '//aios/autil:log', '//aios/autil:string_helper', '//aios/autil:time',
        '//aios/filesystem/fslib/fslib/fs', '//third_party/zookeeper'
    ],
    alwayslink=True
)
cc_library(
    name='fslib_zookeeper_casehelper',
    srcs=[
        'test/FsCaseHelperFactory.cpp', 'test/ZkServer.cpp',
        'test/ZookeeperFsCaseHelper.cpp'
    ],
    hdrs=[
        'test/FsCaseHelperBase.h', 'test/FsCaseHelperFactory.h',
        'test/ZkServer.h', 'test/ZookeeperFsCaseHelper.h'
    ],
    data=['zookeeper_server'],
    include_prefix='fslib/fs/zookeeper',
    linkopts=[],
    visibility=['//visibility:public'],
    deps=[
        ':zfs', '//aios/autil:net', '//aios/autil:time',
        '//aios/filesystem/fslib:fslib-framework'
    ]
)
filegroup(
    name='zookeeper_server',
    srcs=glob(['test/testdata/**'], exclude=['test/testdata/zookeeper/var/**']),
    visibility=['//visibility:public']
)
