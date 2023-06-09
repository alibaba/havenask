package(default_visibility=['//aios/network/gig:__subpackages__'])
load('//bazel:defs.bzl', 'cc_proto')
jni_cpp_srcs = ['GigApi.h', 'GigApi.cpp']
cc_library(
    name='java_headers',
    hdrs=glob(['*.h', 'arpc/*.h']),
    deps=[
        '//aios/network/gig:jni_proto_cc_proto_headers',
        '//aios/network/gig/multi_call/agent:agent_headers',
        '//aios/network/gig/multi_call/config:config_headers',
        '//aios/network/gig/multi_call/service:service_headers',
        '//aios/network/gig/multi_call/stream:stream_headers'
    ]
)
cc_library(
    name='java',
    srcs=glob(['*.cpp', 'arpc/*.cpp'], exclude=jni_cpp_srcs),
    deps=[
        '//aios/network/gig:jni_proto_cc_proto', ':java_headers',
        '@bazel_tools//tools/jdk:jni', '@grpc//:grpc++',
        '//aios/autil:string_helper'
    ],
    alwayslink=True
)
cc_proto(
    name='mock',
    srcs=['test/proto/FakeRpcProto.proto'],
    deps=['//aios/network/arpc:proto']
)
