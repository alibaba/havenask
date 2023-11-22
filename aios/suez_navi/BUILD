package(default_visibility=['//aios/navi:__subpackages__'])
load('//bazel:defs.bzl', 'cc_proto')
cc_library(
    name='suez_navi',
    srcs=glob(['search/*.cpp']),
    hdrs=glob(['search/*.h']),
    copts=['-Werror', '-Wno-aligned-new'],
    include_prefix='suez_navi',
    linkopts=['-L/usr/lib64', '-lpython3.6m'],
    visibility=['//visibility:public'],
    deps=[
        ':suez_navi_health_check', ':suez_navi_resource',
        '//aios/autil:plugin_base',
        '//aios/kmonitor:metrics_report_navi_resource', '//aios/navi',
        '//aios/network/http_arpc', '//aios/suez:suez_index_provider_r',
        '//aios/suez:suez_search_sdk'
    ],
    alwayslink=True
)
cc_library(
    name='suez_navi_resource',
    srcs=glob(['resource/*.cpp']),
    hdrs=glob(['resource/*.h']),
    copts=['-Werror', '-Wno-aligned-new'],
    include_prefix='suez_navi',
    linkopts=['-L/usr/lib64'],
    visibility=['//visibility:public'],
    deps=['//aios/navi'],
    alwayslink=True
)
cc_proto(
    name='health_check_proto',
    srcs=['health_check/HealthCheck.proto'],
    import_prefix='suez_navi',
    visibility=['//visibility:public'],
    deps=['//aios/network/arpc:proto']
)
cc_library(
    name='suez_navi_health_check',
    srcs=glob(['health_check/*.cpp']),
    hdrs=glob(['health_check/*.h']),
    copts=['-Werror', '-Wno-aligned-new'],
    include_prefix='suez_navi',
    linkopts=['-L/usr/lib64'],
    visibility=['//visibility:public'],
    deps=['health_check_proto_cc_proto', '//aios/navi'],
    alwayslink=True
)
cc_library(
    name='suez_navi_search_manager_creator',
    srcs=glob(['search/NaviSearchManagerCreator.cpp']),
    hdrs=glob([]),
    copts=['-Werror', '-Wno-aligned-new'],
    include_prefix='suez_navi',
    linkopts=['-L/usr/lib64', '-lpython3.6m'],
    visibility=['//visibility:public'],
    deps=[':suez_navi'],
    alwayslink=True
)
