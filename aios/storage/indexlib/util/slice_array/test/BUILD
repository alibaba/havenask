load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='slice_array_unittest',
    srcs=[
        'AlignedSliceArrayTest.cpp', 'ByteAlignedSliceArrayTest.cpp',
        'BytesAlignedSliceArrayTest.cpp', 'DefragSliceArrayTest.cpp',
        'SliceArrayTest.cpp'
    ],
    deps=[
        '//aios/storage/indexlib/util/slice_array',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
