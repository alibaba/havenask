package(default_visibility=['//aios/apps/facility/swift:__subpackages__'])
cc_library(
    name='swift_network_public_headers',
    hdrs=['SwiftAdminAdapter.h'],
    include_prefix='swift/network',
    deps=['//aios/apps/facility/swift:swift_version_h']
)
cc_library(
    name='swift_network_admin',
    srcs=[
        'ArpcClient.cpp', 'ClientFileUtil.cpp', 'SwiftAdminAdapter.cpp',
        'SwiftAdminClient.cpp', 'SwiftRpcChannelManager.cpp'
    ],
    hdrs=[
        'ArpcClient.h', 'ClientFileUtil.h', 'SwiftAdminAdapter.h',
        'SwiftAdminClient.h', 'SwiftRpcChannelManager.h'
    ],
    include_prefix='swift/network',
    deps=[
        '//aios/apps/facility/swift:swift_version_h',
        '//aios/apps/facility/swift/auth:swift_authentication',
        '//aios/apps/facility/swift/common:swift_common',
        '//aios/apps/facility/swift/protocol:swift_protocol',
        '//aios/autil:hash_func', '//aios/autil:thread', '//aios/network/arpc',
        '//third_party/zookeeper'
    ]
)
cc_test(
    name='swift_network_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=['//aios/apps/facility/swift:swift_testdata'],
    shard_count=2,
    deps=[
        ':swift_network_admin',
        '//aios/apps/facility/swift/client/fake_client:fake_client_minimal',
        '//aios/filesystem/fslib:fslib-framework', '//aios/unittest_framework'
    ]
)
