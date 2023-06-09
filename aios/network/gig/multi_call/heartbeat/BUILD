package(default_visibility=['//aios/network/gig:__subpackages__'])
public_headers = ['MetaManagerServer.h']
cc_library(name='heartbeat_public_headers', hdrs=public_headers)
cc_library(
    name='heartbeat_headers',
    hdrs=glob(['*.h'], exclude=public_headers),
    deps=[
        ':heartbeat_public_headers',
        '//aios/network/gig/multi_call/config:config_headers'
    ]
)
cc_library(
    name='heartbeat',
    srcs=glob(['*.cpp']),
    deps=[
        ':heartbeat_headers', '@grpc//:grpc++',
        '//aios/network/gig/multi_call/grpc:grpc_headers',
        '//aios/network/gig/multi_call/service:service_headers',
        '//aios/network/gig/multi_call/stream:stream_headers',
        '//aios/network/gig/multi_call/config:config'
    ]
)
