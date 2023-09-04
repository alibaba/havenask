package(default_visibility=['//aios/filesystem/fslib:__subpackages__'])
load('//bazel:bundle.bzl', 'bundle_files')
cc_library(
    name='fsutil_lib',
    srcs=glob(['fsutil/*.cpp']),
    hdrs=glob(['fsutil/*.h']),
    deps=['//aios/filesystem/fslib:fslib-framework', '//aios/autil:cipher'],
    include_prefix='tools',
    visibility=['//visibility:public']
)
cc_binary(
    name='fs_util_bin',
    deps=[':fsutil_lib'],
    visibility=['//visibility:public']
)
filegroup(
    name='fsutil_tools', srcs=['fsutil/fs_util', 'fsutil/class_path_util']
)
bundle_files(
    name='fsutil_util_bin_package',
    prefix='usr/local/bin/',
    srcs=[':fs_util_bin']
)
bundle_files(
    name='fsutil_tools_package',
    prefix='usr/local/bin/',
    strip_prefix='fsutil/',
    srcs=[':fsutil_tools'],
    deps=[':fsutil_util_bin_package'],
    visibility=['//visibility:public'],
    tags=['manual']
)
