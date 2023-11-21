package(default_visibility=['//aios/apps/facility/swift:__subpackages__'])
cc_library(
    name='fake_client_part',
    srcs=glob([
        'FakeSwiftAdminClient.cpp', 'FakeSwiftTransportClient.cpp',
        'FakeChannelManager.cpp', 'FakeRpcChannel.cpp',
        'FakeSwiftAdminAdapter.cpp', 'FakeSwiftSinglePartitionReader.cpp',
        'FakeClientHelper.cpp', 'SwiftWriterImplMock.cpp'
    ]),
    hdrs=glob([
        'FakeSwiftClient.h', 'FakeSwiftTransportClientCreator.h',
        'FakeSwiftAdminClient.h', 'FakeSwiftTransportClient.h',
        'FakeChannelManager.h', 'FakeRpcChannel.h', 'FakeSwiftWriter.h',
        'FakeSwiftAdminAdapter.h', 'FakeSwiftSinglePartitionReader.h',
        'FakeClientHelper.h', 'SwiftWriterImplMock.h',
        'FakeSwiftTransportAdapter.h'
    ]),
    include_prefix='swift/client/fake_client',
    deps=[
        '//aios/apps/facility/swift/client:swift_client_no_transport',
        '//aios/apps/facility/swift/client:transport_client_without_creator'
    ]
)
cc_library(
    name='fake_client_minimal',
    srcs=['FakeSwiftTransportClientCreator.cpp'],
    include_prefix='swift/client/fake_client',
    visibility=['//visibility:public'],
    deps=[':fake_client_part']
)
