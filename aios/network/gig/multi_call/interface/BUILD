package(default_visibility=['//aios/network/gig:__subpackages__'])
closure_headers = ['Closure.h']
cc_library(
    name='closure_headers',
    hdrs=closure_headers,
    deps=[
        '//aios/network/gig/multi_call/config:config_headers',
        '//aios/network/gig/multi_call/common:common_headers',
        '//aios/network/gig/multi_call/util:util_headers'
    ]
)
cc_library(
    name='interface_headers',
    hdrs=glob(['*.h'], exclude=closure_headers),
    deps=[
        ':closure_headers', '//aios/network/gig:common_proto_cc_proto_headers',
        '//aios/network/gig:jni_proto_cc_proto_headers',
        '//aios/network/gig/multi_call/config:config_headers',
        '//aios/network/gig/multi_call/common:common_headers',
        '//aios/network/opentelemetry:opentelemetry'
    ]
)
cc_library(
    name='interface',
    srcs=glob(['*.cpp']),
    deps=[
        ':interface_headers', '//aios/network/gig:agent_proto_cc_proto',
        '//aios/network/gig/multi_call/service:service',
        '//aios/network/gig/multi_call/stream:stream_headers',
        '//aios/network/gig/multi_call/stream:stream_inner_headers',
        '//aios/network/gig/multi_call/common:common',
        '//aios/network/gig/multi_call/util:util',
        '//aios/network/opentelemetry:opentelemetry'
    ],
    alwayslink=True
)
