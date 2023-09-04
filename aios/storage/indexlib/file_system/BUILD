load('//aios/storage:defs.bzl', 'strict_cc_library')
package(
    default_visibility=['//aios/storage/indexlib/file_system:__subpackages__']
)
strict_cc_library(
    name='interface',
    srcs=[],
    hdrs=[
        'Directory.h', 'DirectoryOption.h', 'FileSystemCreator.h',
        'FileSystemDefine.h', 'FileSystemOptions.h', 'IDirectory.h',
        'IFileSystem.h', 'LifecycleConfig.h', 'ListOption.h',
        'MergeDirsOption.h', 'MountOption.h', 'ReaderOption.h',
        'RemoveOption.h', 'WriterOption.h'
    ],
    visibility=['//visibility:public'],
    deps=[
        ':ErrorCode', ':FSResult',
        '//aios/storage/indexlib/file_system/file:interface',
        '//aios/storage/indexlib/file_system/fslib:interface',
        '//aios/storage/indexlib/file_system/load_config:interface'
    ]
)
strict_cc_library(
    name='common',
    srcs=[
        'FileBlockCache.cpp', 'FileBlockCacheContainer.cpp',
        'FileBlockCacheTaskItem.cpp', 'FileSystemMetrics.cpp',
        'FileSystemMetricsReporter.cpp', 'FileSystemOptions.cpp',
        'LifecycleConfig.cpp', 'LifecycleTable.cpp'
    ],
    hdrs=[
        'BlockCacheMetrics.h', 'FileBlockCache.h', 'FileBlockCacheContainer.h',
        'FileBlockCacheTaskItem.h', 'FileInfo.h', 'FileSystemMetrics.h',
        'FileSystemMetricsReporter.h', 'LifecycleTable.h', 'StorageMetrics.h'
    ],
    deps=[
        ':JsonUtil', ':interface', '//aios/aios/common/beeper',
        '//aios/autil:synchronized_queue', '//aios/autil:timeout_terminator',
        '//aios/storage/indexlib/file_system/fslib',
        '//aios/storage/indexlib/file_system/load_config'
    ]
)
strict_cc_library(
    name='entry_table',
    srcs=[
        'EntryMeta.cpp', 'EntryTable.cpp', 'EntryTableBuilder.cpp',
        'EntryTableJsonizable.cpp', 'EntryTableMerger.cpp', 'IndexFileList.cpp',
        'LegacyVersion.cpp', 'PackageMetaInfo.cpp'
    ],
    hdrs=[
        'EntryMeta.h', 'EntryTable.h', 'EntryTableBuilder.h',
        'EntryTableJsonizable.h', 'EntryTableMerger.h', 'IndexFileList.h',
        'LegacyVersion.h', 'PackageMetaInfo.h'
    ],
    deps=[
        ':common', '//aios/aios/common/beeper', '//aios/autil:NoCopyable',
        '//aios/storage/indexlib/base:PathUtil',
        '//aios/storage/indexlib/file_system/fslib',
        '//aios/storage/indexlib/file_system/load_config:interface',
        '//aios/storage/indexlib/file_system/package:meta'
    ]
)
strict_cc_library(
    name='byte_slice_rw',
    srcs=['ByteSliceReader.cpp', 'ByteSliceWriter.cpp'],
    hdrs=['ByteSliceReader.h', 'ByteSliceWriter.h'],
    visibility=[
        '//aios/khronos_table:__subpackages__',
        '//aios/storage/indexlib:__subpackages__'
    ],
    deps=[
        ':common', '//aios/storage/indexlib/base:constants',
        '//aios/storage/indexlib/file_system/file:headers',
        '//aios/storage/indexlib/util/byte_slice_list'
    ]
)
strict_cc_library(
    name='deploy',
    srcs=['IndexFileDeployer.cpp'],
    hdrs=['DeployIndexMeta.h', 'IndexFileDeployer.h'],
    visibility=['//aios/storage/indexlib:__subpackages__'],
    deps=[
        ':ErrorCode', ':FSResult', ':JsonUtil', ':entry_table',
        '//aios/autil:json', '//aios/autil:log',
        '//aios/storage/indexlib/base:PathUtil',
        '//aios/storage/indexlib/file_system/fslib'
    ]
)
strict_cc_library(
    name='file_system',
    srcs=[
        'Directory.cpp', 'DirectoryImpl.cpp', 'DiskStorage.cpp',
        'FenceDirectory.cpp', 'FileSystemCreator.cpp', 'LinkDirectory.cpp',
        'LocalDirectory.cpp', 'LogicalFileSystem.cpp', 'MemDirectory.cpp',
        'MemStorage.cpp', 'PhysicalDirectory.cpp', 'Storage.cpp',
        'TempDirectory.cpp'
    ],
    hdrs=[
        'DirectoryImpl.h', 'FenceDirectory.h', 'LinkDirectory.h',
        'LocalDirectory.h', 'LogicalFileSystem.h', 'MemDirectory.h',
        'PhysicalDirectory.h', 'TempDirectory.h'
    ],
    visibility=['//aios/storage:__subpackages__'],
    deps=[
        ':DiskStorage', ':MemStorage', ':Storage', ':common', ':deploy',
        ':entry_table', ':interface',
        '//aios/storage/indexlib/file_system/archive',
        '//aios/storage/indexlib/file_system/file',
        '//aios/storage/indexlib/file_system/flush',
        '//aios/storage/indexlib/file_system/fslib',
        '//aios/storage/indexlib/file_system/load_config',
        '//aios/storage/indexlib/file_system/package'
    ]
)
strict_cc_library(name='FSResult', srcs=[], deps=[':ErrorCode'])
strict_cc_library(
    name='ErrorCode',
    srcs=[],
    deps=[
        '//aios/filesystem/fslib:fslib-framework',
        '//aios/storage/indexlib/base:Status',
        '//aios/storage/indexlib/util:Exception'
    ]
)
strict_cc_library(name='FileSystemDefine', srcs=[], deps=[])
strict_cc_library(name='Storage', srcs=[], deps=[':entry_table'])
strict_cc_library(name='MemStorage', srcs=[])
strict_cc_library(name='DiskStorage', srcs=[])
strict_cc_library(
    name='JsonUtil',
    deps=[
        ':interface', '//aios/autil:synchronized_queue',
        '//aios/storage/indexlib/file_system/fslib'
    ]
)
