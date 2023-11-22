package(default_visibility=['//aios/apps/facility/swift:__subpackages__'])
cc_library(
    name='swift_common_public_headers',
    hdrs=[
        'Common.h', 'FieldGroupReader.h', 'FieldGroupWriter.h', 'FieldSchema.h',
        'MessageInfo.h', 'PathDefine.h', 'ProgressUtil.h', 'RangeUtil.h',
        'SchemaReader.h', 'SchemaWriter.h'
    ],
    include_prefix='swift/common',
    visibility=[
        '//aios/apps/facility/dispatch_framework:__subpackages__',
        '//aios/apps/facility/swift:__subpackages__'
    ],
    deps=[
        '//aios/autil:dynamic_buf', '//aios/autil:singleton',
        '//aios/autil:span', '//aios/filesystem/fslib:fslib-framework'
    ]
)
cc_library(
    name='swift_common',
    srcs=glob(['*.cpp'],
              exclude=[
                  'FilePair.cpp', 'ProgressUtil.cpp',
                  'TopicWriterController.cpp', 'SingleTopicWriterController.cpp'
              ]),
    hdrs=glob(['*.h'],
              exclude=[
                  'FilePair.h', 'ProgressUtil.h', 'TopicWriterController.h',
                  'SingleTopicWriterController.h'
              ]),
    include_prefix='swift/common',
    deps=[
        '//aios/apps/facility/cm2/cm_basic',
        '//aios/apps/facility/swift/protocol:swift_protocol_public_headers',
        '//aios/autil:json', '//aios/autil:singleton',
        '//aios/autil:string_helper', '//aios/autil:zlib',
        '//aios/filesystem/fslib:fslib-framework'
    ]
)
cc_library(
    name='swift_common_progress',
    srcs=glob(['ProgressUtil.cpp']),
    hdrs=glob(['ProgressUtil.h']),
    include_prefix='swift/common',
    deps=[
        '//aios/apps/facility/swift/protocol:swift_protocol',
        '//aios/apps/facility/swift/protocol:swift_protocol_public_headers'
    ]
)
cc_test(
    name='swift_common_test',
    srcs=(
        glob(['test/*Test.cpp']) +
        ['test/FakeFslibFile.cpp', 'test/FakeFslibFile.h']
    ),
    copts=['-fno-access-control'],
    data=['//aios/apps/facility/swift:swift_testdata'],
    deps=[
        '//aios/apps/facility/swift:swift_test_h',
        '//aios/apps/facility/swift/common:swift_common',
        '//aios/apps/facility/swift/common:swift_common_progress',
        '//aios/apps/facility/swift/common:swift_file_pair',
        '//aios/apps/facility/swift/common:topic_writer_controller',
        '//aios/apps/facility/swift/config:swift_config',
        '//aios/apps/facility/swift/protocol:swift_protocol',
        '//aios/apps/facility/swift/util:swift_util',
        '//aios/filesystem/fslib/fslib/fs/zookeeper:fslib_zookeeper_casehelper',
        '//aios/unittest_framework'
    ]
)
cc_library(
    name='swift_file_pair',
    srcs=glob(['FilePair.cpp']),
    hdrs=glob(['FilePair.h']),
    include_prefix='swift/common',
    deps=[
        '//aios/apps/facility/swift/protocol:swift_protocol',
        '//aios/filesystem/fslib:fslib-framework'
    ]
)
cc_library(
    name='topic_writer_controller',
    srcs=glob(['TopicWriterController.cpp', 'SingleTopicWriterController.cpp']),
    hdrs=glob(['TopicWriterController.h', 'SingleTopicWriterController.h']),
    include_prefix='swift/common',
    deps=[
        '//aios/apps/facility/swift/protocol:swift_protocol',
        '//aios/apps/facility/swift/util:swift_util',
        '//aios/filesystem/fslib:fslib-framework'
    ]
)
