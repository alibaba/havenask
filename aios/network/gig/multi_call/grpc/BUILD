package(default_visibility=['//aios/network/gig:__subpackages__'])
cc_library(
    name='grpc_headers',
    hdrs=glob(['*.h', 'server/*.h', 'client/*.h']),
    deps=[
        '//aios/network/gig/multi_call/config:config_headers',
        '//aios/network/gig/multi_call/rpc:rpc_headers',
        '//aios/network/gig/multi_call/agent:agent_headers',
        '//aios/network/gig/multi_call/interface:interface_headers'
    ]
)
cc_library(
    name='grpc',
    srcs=glob(['server/*.cpp', 'client/*.cpp']),
    deps=[
        ':grpc_headers', '@grpc//:grpc++',
        '//aios/network/gig/multi_call/stream:stream',
        '//aios/network/gig/multi_call/stream:stream_inner_headers',
        '//aios/network/gig/multi_call/service:service_headers'
    ]
)
