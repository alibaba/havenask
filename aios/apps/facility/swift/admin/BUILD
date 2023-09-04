package(default_visibility=['//aios/apps/facility/swift:__subpackages__'])
cc_library(
    name='swift_admin_lib',
    srcs=glob(['*.cpp', 'modules/*.cpp'],
              exclude=[
                  'AdminWorkerCreator.cpp', 'AdminServiceImpl.cpp',
                  'AdminWorker.cpp'
              ]),
    hdrs=glob(['*.h', 'modules/*.h'],
              exclude=['AdminServiceImpl.h', 'AdminWorker.h']),
    copts=['-Werror'],
    include_prefix='swift/admin',
    deps=[
        '//aios/apps/facility/cm2/cm_server/proto:cm_server_cc_proto',
        '//aios/apps/facility/swift:swift_version_h',
        '//aios/apps/facility/swift/common:swift_common',
        '//aios/apps/facility/swift/common:topic_writer_controller',
        '//aios/apps/facility/swift/config:swift_config',
        '//aios/apps/facility/swift/heartbeat:swift_heartbeat_lib',
        '//aios/apps/facility/swift/monitor:swift_monitor',
        '//aios/apps/facility/swift/network:swift_network_admin',
        '//aios/apps/facility/swift/util:swift_util', '//aios/autil:bitmap',
        '//aios/autil:result', '//aios/filesystem/fslib:fslib-framework',
        '//aios/libcarbon3:master_framework', '@curl'
    ],
    alwayslink=1
)
cc_library(
    name='swift_admin_worker',
    srcs=glob(['AdminWorker.cpp']),
    hdrs=glob(['AdminWorker.h']),
    copts=['-Werror'],
    include_prefix='swift/admin',
    deps=[
        '//aios/apps/facility/swift/admin:swift_admin_service',
        '//aios/apps/facility/swift/heartbeat:swift_heartbeat_service'
    ]
)
cc_library(
    name='swift_admin_service',
    srcs=glob(['AdminServiceImpl.cpp']),
    hdrs=glob(['AdminServiceImpl.h']),
    copts=['-Werror'],
    include_prefix='swift/admin',
    deps=['//aios/apps/facility/swift/admin:swift_admin_lib']
)
cc_binary(
    name='swift_admin',
    srcs=['AdminWorkerCreator.cpp'],
    copts=['-Werror'],
    visibility=['//visibility:public'],
    deps=[
        ':swift_admin_lib', ':swift_admin_worker',
        '//aios/worker_framework:worker_base_main'
    ]
)
cc_library(
    name='swift_admin_test_lib',
    testonly=True,
    srcs=glob(['test/MessageConstructor.cpp']),
    hdrs=glob(['test/MockSysController.h', 'test/MessageConstructor.h']),
    include_prefix='swift/admin',
    deps=[
        ':swift_admin_service', ':swift_admin_worker',
        '//aios/apps/facility/swift/protocol:fake_protocol'
    ]
)
cc_test(
    name='swift_admin_test',
    srcs=(
        glob([
            'test/TopicInStatusTestHelper.h',
            'test/TopicInStatusTestHelper.cpp', 'test/*Test.cpp',
            'modules/test/MockBaseModule.h'
        ]) + glob(['modules/test/*Test.cpp'],
                  exclude=['modules/test/MetaInfoReplicatorModuleTest.cpp'])
    ),
    copts=['-fno-access-control', '-Iaios/apps/facility'],
    data=[
        '//aios/apps/facility/swift:swift_testdata',
        '//aios/filesystem/fslib:fslib_plugin_mock'
    ],
    flaky=True,
    shard_count=2,
    deps=[
        ':swift_admin_test_lib', '//aios/apps/facility/swift:swift_test_h',
        '//aios/apps/facility/swift/common:swift_common',
        '//aios/filesystem/fslib/fslib/fs/zookeeper:fslib_zookeeper_casehelper',
        '//aios/unittest_framework'
    ]
)
