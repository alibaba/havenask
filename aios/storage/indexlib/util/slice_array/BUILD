load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='slice_array',
    srcs=[
        'ByteAlignedSliceArray.cpp', 'BytesAlignedSliceArray.cpp',
        'DefragSliceArray.cpp'
    ],
    hdrs=[
        'AlignedSliceArray.h', 'ByteAlignedSliceArray.h',
        'BytesAlignedSliceArray.h', 'DefragSliceArray.h', 'SliceArray.h'
    ],
    deps=[
        '//aios/storage/indexlib/util:Exception',
        '//aios/storage/indexlib/util:VectorTyped',
        '//aios/storage/indexlib/util:mmap_pool',
        '//aios/storage/indexlib/util/memory_control'
    ]
)
