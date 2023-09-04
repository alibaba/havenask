package(default_visibility=['//aios/apps/facility/swift:__subpackages__'])
cc_library(
    name='swift_client_public_headers',
    hdrs=[
        'MessageInfo.h', 'RangeUtil.h', 'SwiftClient.h', 'SwiftClientConfig.h',
        'SwiftPartitionStatus.h', 'SwiftReader.h', 'SwiftReaderConfig.h',
        'SwiftTopicStreamReader.h', 'SwiftWriter.h', 'SwiftWriterConfig.h',
        'helper/CheckpointBuffer.h', 'helper/SwiftWriterAsyncHelper.h'
    ],
    include_prefix='swift/client',
    deps=[
        '//aios/autil:json', '//aios/autil:log', '//aios/autil:result',
        '//aios/autil:thread'
    ]
)
cc_library(
    name='transport_client_without_creator',
    srcs=['Notifier.cpp', 'SwiftTransportClient.cpp', 'TransportClosure.cpp'],
    hdrs=[
        'Notifier.h', 'SwiftTransportAdapter.h', 'SwiftTransportClient.h',
        'SwiftTransportClientCreator.h', 'TransportClosure.h'
    ],
    copts=['-Werror'],
    include_prefix='swift/client',
    deps=[
        '//aios/apps/facility/swift/monitor:swift_monitor',
        '//aios/apps/facility/swift/network:swift_network_admin'
    ],
    alwayslink=True
)
cc_library(
    name='transport_client',
    srcs=['SwiftTransportClientCreator.cpp'],
    hdrs=['SwiftTransportClientCreator.h'],
    copts=['-Werror'],
    include_prefix='swift/client',
    deps=[':transport_client_without_creator'],
    alwayslink=True
)
cc_library(
    name='swift_client_no_transport',
    srcs=glob(['*.cpp', 'helper/*.cpp', 'trace/*.cpp'],
              exclude=[
                  'SwiftTransportClientCreator.cpp', 'SwiftTransportClient.cpp',
                  'TransportClosure.cpp', 'Notifier.cpp'
              ]),
    hdrs=glob(['*.h', 'trace/*.h', 'helper/*.h']),
    copts=['-Werror'],
    include_prefix='swift/client',
    deps=[
        '//aios/apps/facility/swift:swift_version_h',
        '//aios/apps/facility/swift/common:swift_common_progress',
        '//aios/apps/facility/swift/filter:swift_filter',
        '//aios/apps/facility/swift/monitor:swift_monitor',
        '//aios/apps/facility/swift/network:swift_network_admin',
        '//aios/apps/facility/swift/util:swift_util_no_fslib_part',
        '//aios/autil:result'
    ],
    alwayslink=True
)
cc_library(
    name='swift_client_lib',
    srcs=[],
    hdrs=[],
    copts=['-Werror'],
    include_prefix='swift/client',
    deps=[':swift_client_no_transport', ':transport_client'],
    alwayslink=True
)
cc_test(
    name='client_benchmark',
    srcs=glob(['test/benchmark/ReaderBenchmark.cpp']),
    copts=['-fno-access-control'],
    data=[],
    linkopts=[],
    deps=[
        '//aios/apps/facility/swift/client/fake_client:fake_client_minimal',
        '//aios/apps/facility/swift/testlib:mock_swift_client',
        '//aios/apps/facility/swift/util:swift_util',
        '//aios/filesystem/fslib:fslib-framework',
        '//aios/unittest_framework:unittest_benchmark'
    ]
)
