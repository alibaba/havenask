load('//aios/storage:defs.bzl', 'strict_cc_library')
strict_cc_library(
    name='lifecycle',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':LifecycleStrategyFactory', ':LifecycleTableCreator']
)
strict_cc_library(
    name='LifecycleStrategy',
    deps=[
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework:SegmentDescriptions',
        '//aios/storage/indexlib/framework:SegmentStatistics'
    ]
)
strict_cc_library(
    name='DynamicLifecycleStrategy',
    deps=[':LifecycleStrategy', '//aios/storage/indexlib/file_system']
)
strict_cc_library(
    name='StaticLifecycleStrategy',
    deps=[':LifecycleStrategy', '//aios/storage/indexlib/file_system']
)
strict_cc_library(
    name='LifecycleStrategyFactory',
    deps=[
        ':DynamicLifecycleStrategy', ':StaticLifecycleStrategy',
        '//aios/storage/indexlib/file_system'
    ]
)
strict_cc_library(
    name='LifecycleTableCreator',
    deps=[
        ':LifecycleStrategyFactory', '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework:SegmentDescriptions',
        '//aios/storage/indexlib/framework:SegmentStatistics',
        '//aios/storage/indexlib/framework:Version'
    ]
)
