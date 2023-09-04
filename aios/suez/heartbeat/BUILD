package(default_visibility=['//aios/suez:__subpackages__'])
load('//bazel:defs.bzl', 'cc_proto')
cc_proto(
    name='heartbeat_proto',
    srcs=['SuezHB.proto'],
    import_prefix='suez/heartbeat',
    deps=['//aios/network/arpc:proto']
)
cc_proto(
    name='debug_proto',
    srcs=['Debug.proto'],
    import_prefix='suez/heartbeat',
    deps=[':heartbeat_proto']
)
cc_library(
    name='heartbeat',
    srcs=['HeartbeatManager.cpp', 'HeartbeatTarget.cpp'],
    hdrs=['HeartbeatManager.h', 'HeartbeatTarget.h'],
    include_prefix='suez/heartbeat',
    deps=[
        ':heartbeat_proto_cc_proto', '//aios/autil:closure_guard',
        '//aios/suez/common', '//aios/suez/sdk:RpcServer',
        '//aios/suez/sdk:hb_interface', '//aios/worker_framework'
    ]
)
