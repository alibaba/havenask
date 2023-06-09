package(default_visibility=['//aios/plugins/havenask_plugins:__subpackages__'])
load(
    '@rules_cc//examples:experimental_cc_shared_library.bzl',
    'cc_shared_library'
)
load('//bazel:defs.bzl', 'gen_conf_lib')
load('//bazel:bundle.bzl', 'bundle_files')
bundle_files(
    name='processor_example_package',
    prefix='usr/local/lib64',
    srcs=[':processor_example'],
    deps=[],
    visibility=['//visibility:public'],
    tags=['manual']
)
cc_shared_library(
    name='processor_example',
    visibility=['//visibility:public'],
    roots=[':build_plugins_common'],
    preloaded_deps=[
        '//aios/apps/facility/build_service:bs_processor',
        '//aios/apps/facility/build_service:bs_analyzer',
        '//aios/apps/facility/build_service:bs_document'
    ],
    static_deps=[':build_plugins_common']
)
cc_library(
    name='build_plugins_common',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    deps=[
        '//aios/apps/facility/build_service:bs_processor',
        '//aios/apps/facility/build_service:bs_document'
    ],
    alwayslink=True,
    visibility=['//visibility:public'],
    copts=['-mfma', '-mf16c']
)
