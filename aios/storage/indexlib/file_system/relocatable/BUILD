load('//aios/storage:defs.bzl', 'strict_cc_library')
package(default_visibility=['//aios/storage/indexlib:__subpackages__'])
strict_cc_library(
    name='relocator',
    srcs=['RelocatableFolder.cpp', 'Relocator.cpp'],
    hdrs=['RelocatableFolder.h', 'Relocator.h'],
    deps=[
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/file_system'
    ]
)
