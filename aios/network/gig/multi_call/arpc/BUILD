package(default_visibility=['//aios/network/gig:__subpackages__'])
arpc_public_headers = ['CommonClosure.h', 'ArpcClosure.h']
cc_library(
    name='arpc_public_headers',
    hdrs=arpc_public_headers,
    deps=['//aios/network/gig/multi_call/rpc:rpc_public_headers']
)
cc_library(
    name='arpc_headers',
    hdrs=glob(['*.h'], exclude=arpc_public_headers),
    deps=[
        ':arpc_public_headers', '//aios/network/gig/multi_call/rpc:rpc_headers'
    ]
)
cc_library(
    name='arpc',
    srcs=glob(['*.cpp']),
    deps=[
        ':arpc_headers',
        '//aios/network/gig/multi_call/interface:interface_headers',
        '//aios/network/gig/multi_call/service:service_headers',
        '//aios/network/gig/multi_call/agent:agent_headers',
        '//aios/network/gig/multi_call/stream:stream_headers',
        '//aios/network/gig/multi_call/util:util',
        '//aios/network/http_arpc:http_arpc', '//aios/network/arpc:arpc'
    ]
)
