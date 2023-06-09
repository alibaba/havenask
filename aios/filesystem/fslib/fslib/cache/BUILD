package(default_visibility=['//aios/filesystem/fslib:__subpackages__'])
load('//bazel:defs.bzl', 'nosandbox_test')
cc_library(
    name='cache',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='fslib/cache',
    deps=[
        '//aios/autil:cache2', '//aios/filesystem/fslib/fslib:common',
        '//aios/filesystem/fslib/fslib/util',
        '//aios/kmonitor:kmonitor_client_cpp',
        '//aios/kmonitor:kmonitor_cpp_adapter'
    ]
)
nosandbox_test(
    name='cache_directio_test',
    tags=['no-remote-exec'],
    deps=[':cache_directio_test_lib']
)
