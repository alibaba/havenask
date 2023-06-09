load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
indexlib_cc_library(
    name='hooks',
    srcs=[],
    hdrs=[],
    visibility=['//aios/storage:__subpackages__'],
    deps=[':TabletHooksCreator']
)
indexlib_cc_library(
    name='TabletHooksCreator',
    deps=[
        ':ITabletDeployerHook', '//aios/autil:NoCopyable',
        '//aios/storage/indexlib/base:Types',
        '//aios/storage/indexlib/util:Singleton'
    ]
)
indexlib_cc_library(
    name='ITabletDeployerHook',
    srcs=[],
    deps=['//aios/autil:NoCopyable', '//aios/storage/indexlib/base:Types']
)
