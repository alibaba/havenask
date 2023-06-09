load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
indexlib_cc_library(
    name='lifecycle',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[':LifecycleStrategyFactory', ':LifecycleTableCreator']
)
indexlib_cc_library(
    name='LifecycleStrategy',
    srcs=[],
    deps=[
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/file_system'
    ]
)
indexlib_cc_library(
    name='DynamicLifecycleStrategy',
    deps=[
        ':LifecycleStrategy', ':LifecycleTableCreator',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework:SegmentDescriptions',
        '//aios/storage/indexlib/framework:SegmentStatistics'
    ]
)
indexlib_cc_library(
    name='StaticLifecycleStrategy',
    deps=[
        ':LifecycleStrategy', ':LifecycleTableCreator',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework:SegmentDescriptions',
        '//aios/storage/indexlib/framework:SegmentStatistics'
    ]
)
indexlib_cc_library(
    name='LifecycleStrategyFactory',
    deps=[
        ':DynamicLifecycleStrategy', ':StaticLifecycleStrategy',
        '//aios/storage/indexlib/file_system'
    ]
)
indexlib_cc_library(
    name='LifecycleTableCreator',
    deps=[
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/file_system',
        '//aios/storage/indexlib/framework:SegmentDescriptions',
        '//aios/storage/indexlib/framework:SegmentStatistics',
        '//aios/storage/indexlib/framework:Version'
    ]
)
