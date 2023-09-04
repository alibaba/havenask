package(default_visibility=['//aios/network/gig:__subpackages__'])
cc_library(
    name='subscribe_headers',
    hdrs=glob(['*.h']),
    deps=[
        '//aios/network/gig/multi_call/config:config_headers',
        '//aios/network/gig/multi_call/metric:metric_headers'
    ]
)
cc_library(
    name='subscribe',
    srcs=glob(['*.cpp', '*.cc'], exclude=['VIPSubscribeService.cpp']),
    deps=([
        '//aios/network/gig/multi_call/config:config',
        '//aios/network/gig/multi_call/metric:metric', '//aios/autil:log',
        '//aios/apps/facility/cm2/cm_basic/basic_struct:xds_proto_cc_proto',
        '//aios/filesystem/fslib:fslib-framework', '@grpc//:grpc++',
        ':subscribe_headers', '//aios/apps/facility/cm2/cm_sub'
    ] + [])
)
