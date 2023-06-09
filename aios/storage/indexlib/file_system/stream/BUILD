load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(default_visibility=[':__subpackages__'])
indexlib_cc_library(
    name='file_stream',
    srcs=[
        'FileStream.cpp', 'NormalFileStream.cpp', 'BlockFileStream.cpp',
        'CompressFileStream.cpp', 'FileStreamCreator.cpp'
    ],
    hdrs=[
        'FileStream.h', 'NormalFileStream.h', 'BlockFileStream.h',
        'CompressFileStream.h', 'FileStreamCreator.h'
    ],
    deps=[
        '//aios/autil:memory', '//aios/autil:pack_data',
        '//aios/storage/indexlib/file_system'
    ],
    visibility=['//aios/storage/indexlib:__subpackages__']
)
