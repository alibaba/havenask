package(default_visibility=['//aios/memory/lockless_allocator:__subpackages__'])
load(
    '//aios/memory/lockless_allocator:bundle_version.bzl', 'bundle_version_tar'
)
config_setting(
    name='ll_static_link_mode', define_values={'ll_link_mode': 'static'}
)
cc_library(
    name='ll_alloc',
    srcs=[],
    copts=['-fPIC', '-fno-sanitize=all'],
    linkstatic=0,
    visibility=['//visibility:public'],
    deps=select({
        ':ll_static_link_mode': [':lockless_allocator'],
        '//conditions:default': [':lockless_api']
    }),
    alwayslink=True
)
cc_library(
    name='lockless_api_header',
    hdrs=glob(['MallocPoolScope.h', 'LocklessApi.h', 'LocklessStat.h']),
    copts=['-Werror', '-Wno-aligned-new'],
    include_prefix='lockless_allocator',
    visibility=['//visibility:public'],
    deps=[]
)
cc_library(
    name='lockless_api',
    srcs=glob(['MallocPoolScope.cpp', 'LocklessApi.cpp']),
    copts=['-fPIC', '-fno-sanitize=all'],
    include_prefix='lockless_allocator',
    visibility=['//visibility:public'],
    deps=[':lockless_api_header'],
    alwayslink=True
)
bundle_version_tar(
    name='ll_alloc_pkg',
    srcs=['ll_alloc_so'],
    extension='1.0.5',
    visibility=['//visibility:public']
)
