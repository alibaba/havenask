load('//aios/storage:defs.bzl', 'strict_cc_library')
strict_cc_library(
    name='config',
    srcs=['PackageFileTagConfig.cpp', 'PackageFileTagConfigList.cpp'],
    hdrs=['PackageFileTagConfig.h', 'PackageFileTagConfigList.h'],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=['//aios/storage/indexlib/file_system/load_config']
)
strict_cc_library(
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
strict_cc_library(
    name='package',
    srcs=[
        'BufferedPackageFileWriter.cpp', 'DirectoryMerger.cpp',
        'PackageDiskStorage.cpp', 'PackageFileFlushOperation.cpp',
        'PackageMemStorage.cpp'
    ],
    hdrs=[
        'BufferedPackageFileWriter.h', 'DirectoryMerger.h',
        'PackageDiskStorage.h', 'PackageFileFlushOperation.h',
        'PackageFileWriter.h', 'PackageMemStorage.h'
    ],
    visibility=['//aios/storage/indexlib/file_system:__subpackages__'],
    deps=[
        ':MergePackageUtil', ':config', ':meta',
        '//aios/storage/indexlib/file_system:DiskStorage',
        '//aios/storage/indexlib/file_system:MemStorage',
        '//aios/storage/indexlib/file_system:Storage',
        '//aios/storage/indexlib/file_system:entry_table',
        '//aios/storage/indexlib/file_system/flush'
    ]
)
strict_cc_library(name='MergePackageMeta', srcs=[])
strict_cc_library(name='PackagingPlan', deps=['//aios/autil:json'])
strict_cc_library(
    name='MergePackageUtil',
    deps=[
        ':meta', '//aios/storage/indexlib/file_system:FSResult',
        '//aios/storage/indexlib/file_system:interface',
        '//aios/storage/indexlib/util:path_util'
    ]
)
