package(default_visibility=['//aios/apps/facility/swift:__subpackages__'])
load('//bazel:defs.bzl', 'cc_proto')
load('@flatbuffers//:build_defs.bzl', 'flatbuffer_library_public')
flatbuffer_library_public(
    name='swift_message_fbs',
    srcs=['SwiftMessage.fbs'],
    outs=['SwiftMessage_generated.h'],
    language_flag='-c'
)
cc_library(
    name='swift_message_fbs_h',
    hdrs=['SwiftMessage_generated.h'],
    include_prefix='swift/protocol'
)
cc_proto(
    name='swift_proto',
    srcs=glob(['*.proto']),
    import_prefix='swift/protocol',
    visibility=['//visibility:public'],
    deps=['//aios/network/arpc:proto']
)
cc_library(
    name='swift_protocol_headers',
    hdrs=glob(['*.h']),
    include_prefix='swift/protocol',
    deps=[':swift_message_fbs_h', ':swift_proto_cc_proto_headers']
)
cc_library(
    name='swift_protocol_public_headers',
    hdrs=[
        'AdminRequestResponse.pb.h', 'Common.pb.h', 'ErrCode.pb.h',
        'Heartbeat.pb.h', 'SwiftMessage.pb.h'
    ],
    include_prefix='swift/protocol',
    deps=['@com_google_protobuf//:protobuf_headers']
)
cc_library(
    name='swift_protocol',
    srcs=glob(['*.cpp']),
    deps=[
        ':swift_proto_cc_proto', ':swift_protocol_headers',
        '//aios/apps/facility/swift/common:swift_common', '@flatbuffers'
    ]
)
flatbuffer_library_public(
    name='swift_test_fbs',
    srcs=['test/swift_test.fbs'],
    outs=['test/swift_test_generated.h'],
    language_flag='-c'
)
cc_library(
    name='swift_test_fbs_h',
    testonly=True,
    hdrs=['test/swift_test_generated.h'],
    include_prefix='swift/protocol'
)
cc_library(
    name='fake_protocol',
    testonly=True,
    srcs=['test/MessageCreator.cpp'],
    hdrs=['test/MessageCreator.h'],
    include_prefix='swift/protocol',
    deps=['//aios/apps/facility/swift/protocol:swift_protocol']
)
cc_test(
    name='swift_protocol_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=['//aios/apps/facility/swift:swift_testdata'],
    deps=[
        ':fake_protocol', ':swift_test_fbs_h',
        '//aios/apps/facility/swift:swift_test_h', '//aios/autil:data_buffer',
        '//aios/unittest_framework'
    ]
)
