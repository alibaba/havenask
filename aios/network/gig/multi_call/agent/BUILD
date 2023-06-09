package(default_visibility=['//aios/network/gig:__subpackages__'])
agent_public_headers = ['GigAgent.h', 'QueryInfo.h', 'QueryInfoForMock.h']
cc_library(
    name='agent_public_headers',
    hdrs=agent_public_headers,
    deps=[
        '//aios/network/gig/multi_call/common:common_headers',
        '//aios/network/gig:agent_proto_cc_proto_headers'
    ]
)
cc_library(
    name='agent_headers',
    hdrs=glob(['*.h'], exclude=agent_public_headers),
    deps=[
        ':agent_public_headers',
        '//aios/network/gig/multi_call/common:common_headers',
        '//aios/network/gig/multi_call/util:util_headers',
        '//aios/network/gig:agent_proto_cc_proto'
    ]
)
cc_library(
    name='agent',
    srcs=glob(['*.cpp']),
    deps=[
        ':agent_headers', '//aios/network/gig:agent_proto_cc_proto',
        '//aios/network/gig/multi_call/interface:interface_headers',
        '//aios/network/gig/multi_call/service:service_headers',
        '//aios/network/gig/multi_call/stream:stream_headers',
        '//aios/network/gig/multi_call/config:config',
        '//aios/network/gig/multi_call/common:common',
        '//aios/network/gig/multi_call/util:util', '//aios/autil:log'
    ],
    alwayslink=True
)
