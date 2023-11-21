package(default_visibility=['//aios/filesystem/fslib:__subpackages__'])
load('//aios/filesystem/fslib:defs.bzl', 'fslib_enable_async')
cc_library(
    name='header',
    hdrs=(
        glob(['*.h'], exclude=['IOController.h']) +
        fslib_enable_async(['IOController.h'])
    ),
    include_prefix='fslib/fs'
)
cc_library(
    name='fs',
    srcs=glob(['*.cpp']),
    include_prefix='fslib/fs',
    visibility=[
        '//aios/filesystem/fslib:__subpackages__',
        '//aios/network/gig:__subpackages__'
    ],
    deps=[
        '//aios/autil:hash_func', '//aios/autil:lock', '//aios/autil:net',
        '//aios/autil:regex', '//aios/autil:stack_tracer',
        '//aios/filesystem/fslib/fslib:cc_fslib_config_h',
        '//aios/filesystem/fslib/fslib:common',
        '//aios/filesystem/fslib/fslib/cache',
        '//aios/filesystem/fslib/fslib/fs/local',
        '//aios/filesystem/fslib/fslib/util'
    ],
    alwayslink=True
)
