load('//aios/storage/indexlib:defs.bzl', 'indexlib_cc_library')
indexlib_cc_library(
    name='meta',
    srcs=[
        'InnerFileMeta.cpp', 'PackageFileMeta.cpp',
        'VersionedPackageFileMeta.cpp'
    ],
    hdrs=[
        'InnerFileMeta.h', 'PackageFileMeta.h', 'PackageOpenMeta.h',
        'VersionedPackageFileMeta.h'
    ],
    visibility=['//aios/storage/indexlib/file_system:__subpackages__'],
    deps=[
        ':MergePackageMeta', ':PackagingPlan', '//aios/aios/common/beeper',
        '//aios/storage/indexlib/file_system:common',
        '//aios/storage/indexlib/file_system/file:headers',
        '//aios/storage/indexlib/file_system/fslib'
    ]
)
indexlib_cc_library(
    name='package',
    srcs=[
        'BufferedPackageFileWriter.cpp', 'DirectoryMerger.cpp',
        'PackageDiskStorage.cpp', 'PackageFileFlushOperation.cpp',
        'PackageFileTagConfig.cpp', 'PackageFileTagConfigList.cpp',
        'PackageMemStorage.cpp'
    ],
    hdrs=[
        'BufferedPackageFileWriter.h', 'DirectoryMerger.h',
        'PackageDiskStorage.h', 'PackageFileFlushOperation.h',
        'PackageFileTagConfig.h', 'PackageFileTagConfigList.h',
        'PackageFileWriter.h', 'PackageMemStorage.h'
    ],
    visibility=['//aios/storage/indexlib/file_system:__subpackages__'],
    deps=[
        ':MergePackageUtil', ':meta',
        '//aios/storage/indexlib/file_system:DiskStorage',
        '//aios/storage/indexlib/file_system:MemStorage',
        '//aios/storage/indexlib/file_system:Storage',
        '//aios/storage/indexlib/file_system:entry_table',
        '//aios/storage/indexlib/file_system/flush'
    ]
)
indexlib_cc_library(name='MergePackageMeta', srcs=[])
indexlib_cc_library(name='PackagingPlan', deps=['//aios/autil:json'])
indexlib_cc_library(
    name='MergePackageUtil',
    deps=[
        ':meta', '//aios/storage/indexlib/file_system:FSResult',
        '//aios/storage/indexlib/file_system:interface',
        '//aios/storage/indexlib/util:path_util'
    ]
)
