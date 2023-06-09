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
        '@cm2//:cm2', '//aios/filesystem/fslib:fslib-framework',
        '@grpc//:grpc++', ':subscribe_headers'
    ] + [])
)
