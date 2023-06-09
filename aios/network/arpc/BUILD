load('//bazel:defs.bzl', 'cc_proto', 'gen_conf', 'if_rdma')
load('//bazel:bundle.bzl', 'bundle_files')
cc_proto(
    name='proto',
    srcs=['arpc/proto/msg_header.proto', 'arpc/proto/rpc_extensions.proto'],
    deps=[],
    visibility=['//visibility:public'],
    import_prefix='arpc',
    strip_import_prefix='arpc'
)
cc_library(
    name='arpc',
    visibility=['//visibility:public'],
    srcs=(([
        'arpc/DataBufferOutputStream.cpp', 'arpc/MessageSerializable.cpp',
        'arpc/RPCServerAdapter.cpp', 'arpc/UtilFun.cpp',
        'arpc/RPCServerWorkItem.cpp', 'arpc/RPCChannelBase.cpp',
        'arpc/SyncClosure.cpp', 'arpc/RPCMessageSerializable.cpp',
        'arpc/RPCServer.cpp', 'arpc/RPCChannelManagerBase.cpp',
        'arpc/MessageCodec.cpp', 'arpc/RPCServerClosure.cpp', 'arpc/Tracer.cpp',
        'arpc/AdminService.cpp', 'arpc/RPCServerList.cpp'
    ] + [
        'arpc/anet/ANetApp.cpp', 'arpc/anet/ANetRPCMessageCodec.cpp',
        'arpc/anet/ANetRPCServerAdapter.cpp',
        'arpc/anet/ANetRPCServerClosure.cpp',
        'arpc/anet/ANetRPCServerWorkItem.cpp',
        'arpc/anet/ClientPacketHandler.cpp',
        'arpc/anet/SharedClientPacketHandler.cpp', 'arpc/ANetRPCServer.cpp',
        'arpc/ANetRPCController.cpp', 'arpc/ANetRPCChannel.cpp',
        'arpc/ANetRPCChannelManager.cpp', 'arpc/PooledChannelManager.cpp'
    ]) + glob(['arpc/common/*.cpp', 'arpc/util/*.cpp', 'arpc/anet/*.h'])),
    hdrs=glob(['arpc/*.h', 'arpc/anet/*.h', 'arpc/util/*.h',
               'arpc/common/*.h']),
    deps=[
        ':proto_cc_proto', '//aios/network/anet:anet', '//aios/alog:alog',
        '//aios/autil:NoCopyable', '//aios/autil:thread', '//aios/autil:time',
        '//aios/autil:lock', '//aios/autil:env_util', '//aios/autil:lock_free'
    ],
    copts=['-Wformat-truncation=0']
)
test_deps = [':arpc', ':arpc_test_proto_cc_proto']
test_copts = ['-Wno-deprecated-declarations']
cc_proto(
    name='arpc_test_proto',
    srcs=[
        'arpc/proto/test/IntegrationTesting.proto',
        'arpc/proto/test/ArpcPerfTest.proto',
        'arpc/proto/test/ArpcPerfTestArena.proto',
        'arpc/proto/test/TestMessage.proto'
    ],
    deps=[':proto'],
    visibility=['//visibility:public']
)
py_library(
    name='arpc_python',
    srcs=glob(['arpc/python/*.py']),
    visibility=['//visibility:public']
)
