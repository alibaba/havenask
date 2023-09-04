load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=[':__subpackages__'])
strict_cc_library(
    name='file_stream',
    srcs=[
        'BlockFileStream.cpp', 'CompressFileStream.cpp', 'FileStream.cpp',
        'FileStreamCreator.cpp', 'NormalFileStream.cpp'
    ],
    hdrs=[
        'BlockFileStream.h', 'CompressFileStream.h', 'FileStream.h',
        'FileStreamCreator.h', 'NormalFileStream.h'
    ],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        '//aios/autil:memory', '//aios/autil:pack_data',
        '//aios/storage/indexlib/file_system'
    ]
)
