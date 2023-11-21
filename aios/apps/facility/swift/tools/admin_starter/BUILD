package(default_visibility=['//aios/apps/facility/swift:__subpackages__'])
cc_binary(
    name='swift_admin_starter',
    srcs=glob(['SwiftAdminMain.cpp']),
    copts=['-Iswift'],
    visibility=['//visibility:public'],
    deps=[
        ':swift_admin_starter_lib',
        '//aios/apps/facility/swift/tools:swift_log_setup'
    ]
)
cc_library(
    name='swift_admin_starter_lib',
    srcs=glob(['*.cpp'], exclude=['HippoHelper.cpp', 'SwiftAdminMain.cpp']),
    hdrs=glob(['*.h'], exclude=['HippoHelper.h']),
    include_prefix='swift/tools/admin_starter',
    deps=[
        '//aios/apps/facility/swift/admin:swift_admin_lib',
        '//aios/apps/facility/swift/common:swift_common',
        '//aios/apps/facility/swift/config:swift_config',
        '//aios/apps/facility/swift/monitor:swift_monitor',
        '//aios/apps/facility/swift/util:swift_util', '//aios/libcarbon3:c2'
    ]
)
cc_test(
    name='dispatch_helper_test',
    srcs=glob(['test/*.h', 'test/*Test.cpp'],
              exclude=['test/MockHippoClient.cpp', 'test/HippoHelperTest.cpp']),
    copts=['-fno-access-control', '-Iaios/apps/facility'],
    data=['//aios/apps/facility/swift:swift_testdata'],
    deps=[':swift_admin_starter_lib', '//aios/unittest_framework']
)
