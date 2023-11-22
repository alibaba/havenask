load('//aios/storage:defs.bzl', 'strict_cc_fast_test')
strict_cc_fast_test(
    name='byte_slice_list_unittest',
    srcs=['ByteSliceListIteratorTest.cpp', 'ByteSliceTest.cpp'],
    copts=['-fno-access-control'],
    deps=[
        '//aios/storage/indexlib/util/byte_slice_list',
        '//aios/storage/indexlib/util/testutil:unittest'
    ]
)
