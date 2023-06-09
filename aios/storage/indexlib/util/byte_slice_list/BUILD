load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
indexlib_cc_library(
    name='byte_slice_list',
    srcs=['ByteSliceList.cpp', 'ByteSliceListIterator.cpp'],
    hdrs=['ByteSliceList.h', 'ByteSliceListIterator.h'],
    deps=[
        '//aios/autil:mem_pool_base', '//aios/future_lite',
        '//aios/storage/indexlib/util:DeVirtual',
        '//aios/storage/indexlib/util:Exception'
    ]
)
