package(default_visibility=['//aios/apps/facility/swift:__subpackages__'])
cc_library(
    name='swift_config',
    srcs=glob(['*.cpp'], exclude=['ConfigReader.cpp']),
    hdrs=glob(['*.h'], exclude=['ConfigReader.h', 'ConfigDefine.h']),
    include_prefix='swift/config',
    deps=[
        ':swift_config_define', ':swift_config_reader',
        '//aios/apps/facility/swift/common:swift_common',
        '//aios/apps/facility/swift/protocol:swift_protocol',
        '//aios/autil:json', '//aios/autil:string_helper',
        '//aios/filesystem/fslib:fslib-framework'
    ]
)
cc_library(
    name='swift_config_define',
    hdrs=['ConfigDefine.h'],
    include_prefix='swift/config'
)
cc_library(
    name='swift_config_reader',
    srcs=glob(['ConfigReader.cpp']),
    hdrs=glob(['ConfigReader.h']),
    include_prefix='swift/config',
    deps=[
        '//aios/apps/facility/swift/common:swift_common',
        '//aios/filesystem/fslib:fslib-framework'
    ]
)
cc_test(
    name='swift_config_test',
    srcs=glob(['test/*Test.cpp']),
    copts=['-fno-access-control'],
    data=['//aios/apps/facility/swift:swift_testdata'],
    deps=[
        '//aios/apps/facility/swift:swift_test_h',
        '//aios/apps/facility/swift/config:swift_config',
        '//aios/unittest_framework'
    ]
)
