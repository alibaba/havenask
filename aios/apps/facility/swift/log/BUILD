package(default_visibility=['//aios/apps/facility/swift:__subpackages__'])
cc_library(
    name='swift_log',
    srcs=glob(['*.cpp']),
    hdrs=glob(['*.h']),
    include_prefix='swift/log',
    deps=['//aios/apps/facility/swift/monitor:swift_monitor']
)
cc_test(
    name='swift_log_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=[],
    deps=[
        '//aios/apps/facility/swift/log:swift_log', '//aios/unittest_framework'
    ]
)
