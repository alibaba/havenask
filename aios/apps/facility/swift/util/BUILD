package(default_visibility=['//aios/apps/facility/swift:__subpackages__'])
cc_library(
    name='swift_util_no_fslib_part',
    srcs=glob(['*.cpp'],
              exclude=[
                  'FileUtil.cpp', 'TargetRecorder.cpp',
                  'LocalProcessLauncher.cpp', 'PanguInlineFileUtil.cpp',
                  'MockPanguInlineFileUtil.cpp', 'ZkDataAccessor.cpp'
              ]),
    hdrs=glob(['*.h'],
              exclude=[
                  'FileUtil.h', 'TargetRecorder.h', 'LocalProcessLauncher.h',
                  'PanguInlineFileUtil.h', 'MockPanguInlineFileUtil.h',
                  'ZkDataAccessor.h'
              ]),
    include_prefix='swift/util',
    deps=[
        '//aios/apps/facility/swift/common:swift_file_pair',
        '//aios/apps/facility/swift/filter:swift_filter',
        '//aios/apps/facility/swift/protocol:swift_protocol',
        '//aios/network/arpc', '@snappy'
    ]
)
cc_library(
    name='swift_util',
    srcs=glob([
        'FileUtil.cpp', 'TargetRecorder.cpp', 'LocalProcessLauncher.cpp',
        'ZkDataAccessor.cpp', 'PanguInlineFileUtil.cpp'
    ]),
    hdrs=glob([
        'FileUtil.h', 'TargetRecorder.h', 'LocalProcessLauncher.h',
        'ZkDataAccessor.h', 'PanguInlineFileUtil.h'
    ]),
    include_prefix='swift/util',
    deps=[
        ':swift_util_no_fslib_part', '//aios/filesystem/fslib:fslib-framework',
        '//aios/network/http_arpc'
    ]
)
cc_test(
    name='swift_util_test',
    srcs=glob(['test/*Test.cpp'],
              exclude=['test/LocalProcessLauncherTest.cpp']),
    copts=['-fno-access-control'],
    data=[
        '//aios/apps/facility/swift:swift_testdata',
        '//aios/filesystem/fslib:fslib_plugin_mock'
    ],
    shard_count=3,
    deps=[
        '//aios/apps/facility/swift:swift_test_h',
        '//aios/apps/facility/swift/util:swift_util',
        '//aios/filesystem/fslib/fslib/fs/zookeeper:fslib_zookeeper_casehelper',
        '//aios/unittest_framework'
    ]
)
