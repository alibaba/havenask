package(default_visibility=['//aios/filesystem/fslib:__subpackages__'])
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
