package(default_visibility=['//aios/network/gig:__subpackages__'])
rpc_public_headers = [
    'GigRpcServer.h', 'GigClosure.h', 'GigRpcController.h',
    'ServerBizTopoInfo.h'
]
cc_library(
    name='rpc_public_headers',
    hdrs=rpc_public_headers,
    deps=[
        '//aios/network/gig/multi_call/common:common_headers',
        '@grpc//:grpc++_public_hdrs'
    ]
)
cc_library(
    name='rpc_headers',
    hdrs=glob(['*.h'], exclude=rpc_public_headers),
    deps=[
        ':rpc_public_headers', '//aios/network/gig/multi_call/common:common',
        '@grpc//:grpc++_public_hdrs'
    ]
)
cc_library(
    name='rpc',
    srcs=glob(['*.cpp']),
    deps=([
        ':rpc_headers',
        '//aios/network/gig/multi_call/interface:interface_headers',
        '//aios/network/gig/multi_call/service:service_headers',
        '//aios/network/gig/multi_call/stream:stream_headers',
        '//aios/network/gig/multi_call/heartbeat:heartbeat_headers',
        '//aios/network/gig/multi_call/new_heartbeat:new_heartbeat_headers',
        '//aios/network/gig/multi_call/grpc:grpc',
        '//aios/network/gig/multi_call/arpc:arpc',
        '//aios/network/gig/multi_call/agent:agent',
        '//aios/network/gig/multi_call/config:config',
        '//aios/network/gig/multi_call/util:util', '//aios/autil:log',
        '//aios/autil:net', '//aios/autil:lock_free',
        '//aios/network/http_arpc:http_arpc'
    ] + [])
)
