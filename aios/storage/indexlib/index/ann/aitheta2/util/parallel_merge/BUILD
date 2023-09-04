load('//aios/storage:defs.bzl', 'strict_cc_library')
package(
    default_visibility=[
        '//aios/storage/indexlib/index/ann/aitheta2:__subpackages__'
    ]
)
strict_cc_library(
    name='ParallelReduceMeta',
    srcs=['ParallelReduceMeta.cpp'],
    hdrs=['ParallelReduceMeta.h'],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/file_system'
    ],
    alwayslink=True
)
strict_cc_library(
    name='parallel_merge_util',
    srcs=['ParallelMergeUtil.cpp'],
    hdrs=['ParallelMergeUtil.h'],
    deps=[
        ':ParallelReduceMeta',
        '//aios/storage/indexlib/index/ann/aitheta2:aitheta2_index_common'
    ]
)
