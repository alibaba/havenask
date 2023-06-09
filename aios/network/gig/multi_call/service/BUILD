package(default_visibility=['//aios/network/gig:__subpackages__'])
public_headers = [
    'RetryLimitChecker.h', 'LatencyTimeWindow.h', 'LatencyTimeSnapshot.h',
    'ProtocolCallBack.h', 'CallBack.h'
]
cc_library(name='service_public_headers', hdrs=public_headers)
cc_library(
    name='service_headers',
    hdrs=glob(['*.h'],
              exclude=(['RdmaArpcConnectionPool.h', 'RdmaArpcConnection.h'] +
                       public_headers)),
    deps=[
        ':service_public_headers',
        '//aios/network/gig/multi_call/config:config_headers',
        '//aios/network/gig/multi_call/controller:controller_headers',
        '//aios/network/gig/multi_call/grpc:grpc_headers',
        '//aios/network/gig/multi_call/heartbeat:heartbeat_headers',
        '//aios/network/gig/multi_call/interface:interface_headers',
        '//aios/network/gig/multi_call/metric:metric_headers',
        '//aios/network/gig/multi_call/new_heartbeat:new_heartbeat_headers',
        '//aios/network/gig/multi_call/subscribe:subscribe_headers'
    ]
)
cc_library(
    name='service',
    srcs=glob(['*.cpp'],
              exclude=['RdmaArpcConnectionPool.cpp', 'RdmaArpcConnection.cpp']),
    deps=([
        '//aios/network/gig/multi_call/java:java_headers',
        '//aios/network/gig/multi_call/rpc:rpc',
        '//aios/network/gig/multi_call/heartbeat:heartbeat',
        '//aios/network/gig/multi_call/new_heartbeat:new_heartbeat',
        '//aios/network/gig/multi_call/controller:controller',
        '//aios/network/gig/multi_call/metric:metric',
        '//aios/network/gig/multi_call/stream:stream_headers',
        '//aios/network/gig/multi_call/stream:stream_inner_headers',
        '//aios/network/gig:agent_proto_cc_proto',
        '//aios/network/gig/multi_call/config:config',
        '//aios/network/gig/multi_call/subscribe:subscribe', '//aios/autil:log',
        '//aios/autil:string_helper', ':service_headers'
    ] + [])
)
