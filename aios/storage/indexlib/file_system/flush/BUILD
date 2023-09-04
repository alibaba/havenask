load('//aios/storage:defs.bzl', 'strict_cc_library')
strict_cc_library(
    name='flush',
    srcs=[
        'AsyncDumpScheduler.cpp', 'DirOperationCache.cpp',
        'FileFlushOperation.cpp', 'FlushOperationQueue.cpp',
        'MkdirFlushOperation.cpp', 'SimpleDumpScheduler.cpp',
        'SingleFileFlushOperation.cpp'
    ],
    hdrs=[
        'AsyncDumpScheduler.h', 'DirOperationCache.h', 'DumpScheduler.h',
        'Dumpable.h', 'FileFlushOperation.h', 'FlushOperationQueue.h',
        'MkdirFlushOperation.h', 'SimpleDumpScheduler.h',
        'SingleFileFlushOperation.h'
    ],
    visibility=['//aios/storage/indexlib/file_system:__subpackages__'],
    deps=[
        '//aios/storage/indexlib/file_system:common',
        '//aios/storage/indexlib/file_system/file:base',
        '//aios/storage/indexlib/file_system/fslib',
        '//aios/storage/indexlib/file_system/load_config:interface',
        '//aios/storage/indexlib/file_system/package:meta',
        '//aios/storage/indexlib/util:path_util',
        '//aios/storage/indexlib/util:retry_util'
    ]
)
