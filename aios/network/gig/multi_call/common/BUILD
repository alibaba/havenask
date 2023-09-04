package(default_visibility=['//aios/network/gig:__subpackages__'])
cc_library(
    name='common_headers',
    hdrs=[
        'common.h', 'ErrorCode.h', 'VersionInfo.h', 'ControllerParam.h',
        'HbInfo.h', 'MetaEnv.h', 'QueryResultStatistic.h', 'Spec.h',
        'TopoInfoBuilder.h', 'WorkerNodeInfo.h'
    ],
    deps=[
        '//aios/network/gig:heartbeat_proto_cc_proto_headers',
        '//aios/apps/facility/cm2/cm_basic/basic_struct:proto_cc_proto',
        '//aios/autil:json', '//aios/autil:log', '//aios/autil:common_macros',
        '//aios/autil:time', '//aios/autil:lock', '//aios/autil:string_helper',
        '//aios/autil:base64', '//aios/autil:synchronized_queue',
        '@com_google_protobuf//:protobuf_headers'
    ],
    visibility=['//visibility:public']
)
cc_library(
    name='common',
    srcs=[
        'common.cpp', 'ErrorCode.cpp', 'ControllerParam.cpp', 'HbInfo.cpp',
        'MetaEnv.cpp', 'QueryResultStatistic.cpp', 'Spec.cpp',
        'TopoInfoBuilder.cpp', 'WorkerNodeInfo.cpp'
    ],
    deps=[
        ':common_headers', '//aios/network/gig/multi_call/util:util_headers',
        '//aios/autil:time', '//aios/autil:string_helper', '//aios/autil:log',
        '//aios/autil:common_macros', '//aios/autil:json',
        '//aios/network/gig:heartbeat_proto_cc_proto',
        '//aios/network/gig:agent_proto_cc_proto',
        '//aios/apps/facility/cm2/cm_basic/basic_struct:proto_cc_proto_headers',
        '@com_google_protobuf//:protobuf'
    ]
)
