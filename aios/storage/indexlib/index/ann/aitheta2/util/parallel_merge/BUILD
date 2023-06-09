load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(
    default_visibility=[
        '//aios/storage/indexlib/index/ann/aitheta2:__subpackages__'
    ]
)
indexlib_cc_library(
    name='ParallelReduceMeta',
    srcs=['ParallelReduceMeta.cpp'],
    hdrs=['ParallelReduceMeta.h'],
    deps=[
        '//aios/autil:log', '//aios/autil:json',
        '//aios/storage/indexlib/file_system:file_system'
    ],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    alwayslink=True
)
