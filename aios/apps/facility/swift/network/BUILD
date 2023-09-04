package(default_visibility=['//aios/apps/facility/swift:__subpackages__'])
cc_library(
    name='swift_network_public_headers',
    include_prefix='swift/network',
    hdrs=['SwiftAdminAdapter.h'],
    deps=['//aios/apps/facility/swift:swift_version_h']
)
cc_library(
    name='swift_network_admin',
    include_prefix='swift/network',
    srcs=[
        'ArpcClient.cpp', 'ClientFileUtil.cpp', 'SwiftAdminAdapter.cpp',
        'SwiftAdminClient.cpp', 'SwiftRpcChannelManager.cpp'
    ],
    hdrs=[
        'ArpcClient.h', 'ClientFileUtil.h', 'SwiftAdminAdapter.h',
        'SwiftAdminClient.h', 'SwiftRpcChannelManager.h'
    ],
    deps=[
        '//aios/apps/facility/swift:swift_version_h',
        '//aios/apps/facility/swift/common:swift_common',
        '//aios/apps/facility/swift/protocol:swift_protocol',
        '//aios/apps/facility/swift/auth:swift_authentication',
        '//aios/autil:thread', '//aios/autil:hash_func',
        '//aios/network/arpc:arpc', '//third_party/zookeeper:zookeeper'
    ]
)
cc_test(
    name='swift_network_test',
    srcs=glob(['test/*Test.cpp']),
    data=['//aios/apps/facility/swift:swift_testdata'],
    copts=['-fno-access-control'],
    deps=[
        ':swift_network_admin',
        '//aios/apps/facility/swift/client/fake_client:fake_client_minimal',
        '//aios/unittest_framework:unittest_framework',
        '//aios/filesystem/fslib:fslib-framework'
    ],
    shard_count=2
)
