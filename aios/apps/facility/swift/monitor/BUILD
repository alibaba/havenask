package(default_visibility=['//aios/apps/facility/swift:__subpackages__'])
cc_library(
    name='swift_monitor',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='swift/monitor',
    deps=[
        '//aios/apps/facility/swift/common:swift_common',
        '//aios/apps/facility/swift/util:swift_util', '//aios/autil:bitmap'
    ]
)
cc_test(
    name='swift_monitor_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=['//aios/apps/facility/swift:swift_testdata'],
    shard_count=1,
    deps=[
        ':swift_monitor', '//aios/apps/facility/swift:swift_test_h',
        '//aios/unittest_framework'
    ]
)
