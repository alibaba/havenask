load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
package(
    default_visibility=['//aios/storage/indexlib/file_system:__subpackages__']
)
indexlib_cc_library(
    name='interface',
    srcs=[],
    hdrs=[
        'ArchiveDirectory.h', 'ArchiveFile.h', 'ArchiveFileReader.h',
        'ArchiveFileWriter.h', 'ArchiveFolder.h', 'LineReader.h', 'LogFile.h'
    ]
)
indexlib_cc_library(
    name='archive',
    srcs=[
        'ArchiveDirectory.cpp', 'ArchiveFile.cpp', 'ArchiveFolder.cpp',
        'LineReader.cpp', 'LogFile.cpp'
    ],
    hdrs=[],
    deps=[
        ':interface', '//aios/autil:json', '//aios/autil:lock',
        '//aios/storage/indexlib/file_system:JsonUtil',
        '//aios/storage/indexlib/file_system:interface',
        '//aios/storage/indexlib/file_system/file:headers',
        '//aios/storage/indexlib/util:Algorithm'
    ]
)
