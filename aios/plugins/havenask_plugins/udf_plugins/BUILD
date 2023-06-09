package(default_visibility=['//aios/plugins/havenask_plugins:__subpackages__'])
load(
    '@rules_cc//examples:experimental_cc_shared_library.bzl',
    'cc_shared_library'
)
load('//bazel:defs.bzl', 'gen_conf_lib')
load('//bazel:bundle.bzl', 'bundle_files')
bundle_files(
    name='test_udf_plugins_package',
    prefix='usr/local/lib64',
    srcs=[':test_udf_plugins'],
    deps=[],
    visibility=['//visibility:public'],
    tags=['manual']
)
cc_shared_library(
    name='test_udf_plugins',
    roots=[':sql_plugins_common'],
    visibility=['//visibility:public'],
    preloaded_deps=[],
    static_deps=[':sql_plugins_common']
)
cc_library(
    name='sql_plugins_common',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    deps=['//aios/ha3:havenask'],
    alwayslink=True,
    visibility=['//visibility:public'],
    copts=['-mfma', '-mf16c']
)
