package(default_visibility=['//aios/network/gig:__subpackages__'])
public_headers = [
    'GigStreamBase.h', 'GigClientStream.h', 'GigServerStream.h',
    'GigServerStreamCreator.h', 'GigStreamMessage.h', 'GigStreamRpcInfo.h'
]
cc_library(
    name='stream_headers',
    hdrs=public_headers,
    deps=[
        '//aios/network/gig:stream_proto_cc_proto_headers',
        '//aios/autil:fmt_string_util'
    ]
)
cc_library(
    name='stream_inner_headers',
    hdrs=glob(['*.h'], exclude=public_headers),
    deps=['//aios/network/gig/multi_call/grpc:grpc_headers']
)
cc_library(
    name='stream',
    srcs=glob(['*.cpp']),
    deps=[
        ':stream_headers', '@grpc//:grpc++', ':stream_inner_headers',
        '//aios/network/gig:stream_proto_cc_proto',
        '//aios/network/gig/multi_call/service:service_headers',
        '//aios/kmonitor:kmonitor_client_cpp', '//aios/network/arpc:arpc',
        '//aios/autil:object_tracer'
    ]
)
