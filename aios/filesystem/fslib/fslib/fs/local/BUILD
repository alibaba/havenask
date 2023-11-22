package(default_visibility=['//aios/filesystem/fslib:__subpackages__'])
load('//aios/filesystem/fslib:defs.bzl', 'fslib_enable_async')
cc_library(
    name='local',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='fslib/fs/local',
    visibility=[
        '//aios/filesystem/fslib:__subpackages__',
        '//aios/network/gig:__subpackages__'
    ],
    deps=([
        '//aios/filesystem/fslib/fslib/util:util',
        '//aios/filesystem/fslib/fslib:common', '//aios/autil:log',
        '//aios/autil:lock'
    ] + fslib_enable_async(['//aios/future_lite:future_lite']))
)
