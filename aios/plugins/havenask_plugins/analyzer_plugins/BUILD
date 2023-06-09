package(default_visibility=['//aios/plugins/havenask_plugins:__subpackages__'])
load(
    '@rules_cc//examples:experimental_cc_shared_library.bzl',
    'cc_shared_library'
)
load('//bazel:defs.bzl', 'gen_conf_lib')
load('//bazel:bundle.bzl', 'bundle_files')
bundle_files(
    name='analyzer_plugin_package',
    prefix='usr/local/lib64',
    srcs=[':analyzer_plugin'],
    deps=[],
    visibility=['//visibility:public'],
    tags=['manual']
)
cc_shared_library(
    name='analyzer_plugin',
    visibility=['//visibility:public'],
    roots=[':analyzer'],
    preloaded_deps=[
        '//aios/apps/facility/build_service:bs_analyzer',
        '//aios/filesystem/fslib:fslib-framework'
    ],
    static_deps=['@cppjieba//:cppjieba', '@cppjieba//:limonp']
)
cc_library(
    name='analyzer',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    deps=[
        '//aios/apps/facility/build_service:bs_analyzer',
        '//aios/filesystem/fslib:fslib-framework', '@cppjieba//:cppjieba',
        '@cppjieba//:limonp'
    ],
    alwayslink=True,
    visibility=['//visibility:public']
)
