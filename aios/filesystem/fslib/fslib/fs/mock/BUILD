package(default_visibility=['//aios/filesystem/fslib:__subpackages__'])
load(
    '@rules_cc//examples:experimental_cc_shared_library.bzl',
    'cc_shared_library'
)
cc_shared_library(
    name='fslib_plugin_mock',
    testonly=True,
    preloaded_deps=[
        '//aios/filesystem/fslib:fslib-framework',
        '//aios/kmonitor:kmonitor_client_cpp',
        '//aios/kmonitor:kmonitor_cpp_adapter', '//aios/alog:alog'
    ],
    roots=[':plugin_mock'],
    static_deps=[],
    visibility=['//visibility:public']
)
cc_library(
    name='plugin_mock',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='fslib/fs/mock',
    visibility=['//visibility:public'],
    deps=[
        '//aios/filesystem/fslib:fslib-framework',
        '//aios/kmonitor:kmonitor_client_cpp',
        '//aios/kmonitor:kmonitor_cpp_adapter'
    ],
    alwayslink=1
)
