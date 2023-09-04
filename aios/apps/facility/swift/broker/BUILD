package(default_visibility=['//aios/apps/facility/swift:__subpackages__'])
cc_library(
    name='swift_replication',
    srcs=glob(['replication/*.cpp']),
    hdrs=glob(['replication/*.h']),
    copts=['-Werror'],
    include_prefix='swift/broker',
    deps=[
        '//aios/apps/facility/swift/client:transport_client',
        '//aios/autil:result', '//aios/autil:time'
    ],
    alwayslink=True
)
cc_library(
    name='swift_broker_lib',
    srcs=glob(['service/*.cpp', 'storage/*.cpp', 'storage_dfs/*.cpp'],
              exclude=['service/BrokerWorkerCreator.cpp']),
    hdrs=glob(['service/*.h', 'storage/*.h', 'storage_dfs/*.h']),
    copts=['-Werror'],
    include_prefix='swift/broker',
    deps=[
        ':swift_replication', '//aios/apps/facility/swift:swift_version_h',
        '//aios/apps/facility/swift/auth:swift_authentication',
        '//aios/apps/facility/swift/common:swift_common',
        '//aios/apps/facility/swift/common:topic_writer_controller',
        '//aios/apps/facility/swift/config:swift_config',
        '//aios/apps/facility/swift/heartbeat:swift_heartbeat_lib',
        '//aios/apps/facility/swift/log:swift_log',
        '//aios/apps/facility/swift/monitor:swift_monitor',
        '//aios/apps/facility/swift/network:swift_network_admin',
        '//aios/apps/facility/swift/protocol:swift_protocol',
        '//aios/apps/facility/swift/util:swift_util', '//aios/autil:metric',
        '//aios/autil:string_helper', '//aios/filesystem/fslib:fslib-framework',
        '//aios/worker_framework'
    ]
)
cc_binary(
    name='swift_broker',
    srcs=glob(['service/BrokerWorkerCreator.cpp']),
    copts=['-Werror'],
    visibility=['//visibility:public'],
    deps=[':swift_broker_lib', '//aios/worker_framework:worker_base_main']
)
cc_library(
    name='swift_broker_storage_dfs_test_lib',
    testonly=True,
    srcs=glob(['storage_dfs/test/FakeMessageCommitter.cpp']),
    hdrs=glob([
        'storage_dfs/test/FakeMessageCommitter.h',
        'storage_dfs/test/MockFileManager.h'
    ]),
    include_prefix='swift/broker',
    deps=[':swift_broker_lib']
)
cc_library(
    name='test_message_util',
    testonly=True,
    hdrs=['storage/test/MemoryMessageUtil.h'],
    include_prefix='swift/broker'
)
cc_library(
    name='fake_closure',
    testonly=True,
    hdrs=['service/test/FakeClosure.h'],
    include_prefix='swift/broker'
)
cc_library(
    name='replication_mock',
    testonly=True,
    hdrs=['replication/test/Mock.h'],
    include_prefix='swift/broker'
)
cc_test(
    name='swift_replication_test',
    srcs=glob(['replication/test/*.cpp']),
    copts=['-fno-access-control'],
    deps=[
        ':replication_mock', ':swift_replication', '//aios/unittest_framework'
    ]
)
cc_test(
    name='swift_broker_service_test',
    srcs=glob(['service/test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[
        '//aios/apps/facility/swift:swift_testdata',
        '//aios/filesystem/fslib:fslib_plugin_mock'
    ],
    flaky=True,
    shard_count=1,
    deps=[
        'fake_closure', ':replication_mock', ':swift_broker_lib',
        '//aios/apps/facility/swift:swift_test_h',
        '//aios/apps/facility/swift/protocol:fake_protocol',
        '//aios/filesystem/fslib/fslib/fs/zookeeper:fslib_zookeeper_casehelper',
        '//aios/unittest_framework'
    ]
)
cc_test(
    name='swift_broker_storage_test',
    srcs=glob(['storage/test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=['//aios/apps/facility/swift:swift_testdata'],
    flaky=True,
    deps=[
        'test_message_util', ':fake_closure', ':swift_broker_lib',
        '//aios/apps/facility/swift:swift_test_h', '//aios/unittest_framework'
    ]
)
cc_test(
    name='swift_broker_storage_dfs_test',
    srcs=glob(['storage_dfs/test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[
        '//aios/apps/facility/swift:swift_testdata',
        '//aios/filesystem/fslib:fslib_plugin_mock'
    ],
    deps=[
        ':swift_broker_storage_dfs_test_lib', ':test_message_util',
        '//aios/apps/facility/swift:swift_test_h', '//aios/unittest_framework'
    ]
)
