package(default_visibility=['//aios/navi:__subpackages__'])
load('//bazel:defs.bzl', 'cc_proto')
cc_library(
    name='suez_navi',
    srcs=glob(['search/*.cpp']),
    hdrs=glob(['search/*.h']),
    include_prefix='suez_navi',
    deps=[
        ':suez_navi_resource', ':suez_navi_health_check', '//aios/navi:navi',
        '//aios/suez:suez_search_sdk', '//aios/suez:suez_index_provider_r',
        '//aios/network/http_arpc:http_arpc', '//aios/autil:plugin_base',
        '//aios/kmonitor:metrics_report_navi_resource'
    ],
    copts=['-Werror', '-Wno-aligned-new'],
    linkopts=['-L/usr/lib64', '-lpython3.6m'],
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='suez_navi_resource',
    srcs=glob(['resource/*.cpp']),
    hdrs=glob(['resource/*.h']),
    include_prefix='suez_navi',
    deps=['//aios/navi:navi'],
    copts=['-Werror', '-Wno-aligned-new'],
    linkopts=['-L/usr/lib64'],
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_proto(
    name='health_check_proto',
    import_prefix='suez_navi',
    srcs=['health_check/HealthCheck.proto'],
    deps=['//aios/network/arpc:proto'],
    visibility=['//visibility:public']
)
cc_library(
    name='suez_navi_health_check',
    srcs=glob(['health_check/*.cpp']),
    hdrs=glob(['health_check/*.h']),
    include_prefix='suez_navi',
    deps=['//aios/navi:navi', 'health_check_proto_cc_proto'],
    copts=['-Werror', '-Wno-aligned-new'],
    linkopts=['-L/usr/lib64'],
    visibility=['//visibility:public'],
    alwayslink=True
)
cc_library(
    name='suez_navi_search_manager_creator',
    srcs=glob(['search/NaviSearchManagerCreator.cpp']),
    hdrs=glob([]),
    include_prefix='suez_navi',
    deps=[':suez_navi'],
    copts=['-Werror', '-Wno-aligned-new'],
    linkopts=['-L/usr/lib64', '-lpython3.6m'],
    visibility=['//visibility:public'],
    alwayslink=True
)
