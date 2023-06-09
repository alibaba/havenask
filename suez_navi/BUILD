package(default_visibility=['//navi:__subpackages__'])
load('//bazel:defs.bzl', 'cc_proto', 'copy_target_to')
load(
    '//bazel:bundle.bzl', 'bundle_files', 'bundle_cc_hdrs', 'bundle_tar',
    'bundle_install'
)
cc_library(
    name='suez_navi',
    srcs=glob(['search/*.cpp', 'resource/*.cpp']),
    hdrs=glob(['search/*.h', 'resource/*.h']),
    deps=[
        '//navi:navi', '//aios/suez:suez_search_sdk',
        '//aios/network/http_arpc:http_arpc', '//aios/autil:plugin_base',
        '@suez_turing.turbojet.turing_ops_util//:suez_turing',
        '//aios/ha3:ha3_sql_lib'
    ],
    copts=['-Werror', '-Wno-aligned-new'],
    linkopts=['-L/usr/lib64', '-lpython3.6m'],
    visibility=['//visibility:public'],
    alwayslink=True
)
