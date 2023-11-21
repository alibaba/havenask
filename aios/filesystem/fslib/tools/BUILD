package(default_visibility=['//aios/filesystem/fslib:__subpackages__'])
load('//bazel:bundle.bzl', 'bundle_files')
cc_library(
    name='fsutil_lib',
    srcs=glob(['fsutil/*.cpp']),
    hdrs=glob(['fsutil/*.h']),
    include_prefix='tools',
    visibility=['//visibility:public'],
    deps=['//aios/autil:cipher', '//aios/filesystem/fslib:fslib-framework']
)
cc_library(
    name='dcacheutil_lib',
    srcs=glob(['dcacheutil/*.cpp', 'fsutil/FsUtil.cpp']),
    hdrs=glob(['fsutil/*.h']),
    deps=[
        '//aios/autil:cipher', '//aios/network/arpc:arpc',
        '//aios/filesystem/fslib:fslib-framework',
        '//aios/apps/facility/cm2/cm_basic:cm_basic',
        '//aios/network/arpc:proto_cc_impl'
    ],
    include_prefix='tools',
    visibility=['//visibility:public']
)
cc_binary(
    name='fs_util_bin',
    visibility=['//visibility:public'],
    deps=[':fsutil_lib']
)
cc_binary(
    name='dcache_util_bin',
    deps=[':dcacheutil_lib'],
    visibility=['//visibility:public']
)
filegroup(
    name='fsutil_tools', srcs=['fsutil/class_path_util', 'fsutil/fs_util']
)
filegroup(name='dcacheutil_tools', srcs=['dcacheutil/dcache_util'])
bundle_files(
    name='fsutil_util_bin_package',
    srcs=[':fs_util_bin'],
    prefix='usr/local/bin/'
)
bundle_files(
    name='dcacheutil_util_bin_package',
    prefix='usr/local/bin/',
    srcs=[':dcache_util_bin']
)
bundle_files(
    name='dcache_tools_package',
    prefix='usr/local/bin/',
    strip_prefix='dcacheutil/',
    srcs=[':dcacheutil_tools'],
    deps=[':dcacheutil_util_bin_package'],
    visibility=['//visibility:public'],
    tags=['manual']
)
bundle_files(
    name='fsutil_tools_package',
    srcs=[':fsutil_tools'],
    prefix='usr/local/bin/',
    strip_prefix='fsutil/',
    tags=['manual'],
    visibility=['//visibility:public'],
    deps=[':fsutil_util_bin_package', ':dcache_tools_package']
)
